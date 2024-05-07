use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::future::BoxFuture;
use tokio::sync::{oneshot, watch};
use zksync_dal::Core;
use zksync_db_connection::connection_pool::ConnectionPool;
use zksync_types::L1BatchNumber;

use crate::{
    state_keeper::{updates::UpdatesManager, StateKeeperOutputHandler},
    vm_runner::VmRunnerStorageLoader,
};

#[async_trait]
pub trait OutputHandlerFactory: Debug + Send {
    async fn create_handler(
        &mut self,
        l1_batch_number: L1BatchNumber,
    ) -> anyhow::Result<Box<dyn StateKeeperOutputHandler>>;
}

pub struct ConcurrentOutputHandlerFactory<L: VmRunnerStorageLoader, F: OutputHandlerFactory> {
    pool: ConnectionPool<Core>,
    state: Arc<DashMap<L1BatchNumber, oneshot::Receiver<BoxFuture<'static, anyhow::Result<()>>>>>,
    loader: L,
    factory: F,
}

impl<L: VmRunnerStorageLoader, F: OutputHandlerFactory> Debug
    for ConcurrentOutputHandlerFactory<L, F>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrentOutputHandlerFactory")
            .field("pool", &self.pool)
            .field("loader", &self.loader)
            .field("factory", &self.factory)
            .finish()
    }
}

impl<L: VmRunnerStorageLoader + Clone, F: OutputHandlerFactory>
    ConcurrentOutputHandlerFactory<L, F>
{
    pub fn new(
        pool: ConnectionPool<Core>,
        loader: L,
        factory: F,
    ) -> (Self, ConcurrentOutputHandlerFactoryTask<L>) {
        let state = Arc::new(DashMap::new());
        let task = ConcurrentOutputHandlerFactoryTask {
            pool: pool.clone(),
            loader: loader.clone(),
            state: state.clone(),
        };
        (
            Self {
                pool,
                state,
                loader,
                factory,
            },
            task,
        )
    }
}

#[async_trait]
impl<L: VmRunnerStorageLoader, F: OutputHandlerFactory> OutputHandlerFactory
    for ConcurrentOutputHandlerFactory<L, F>
{
    async fn create_handler(
        &mut self,
        l1_batch_number: L1BatchNumber,
    ) -> anyhow::Result<Box<dyn StateKeeperOutputHandler>> {
        let mut conn = self.pool.connection_tagged(L::name()).await?;
        let latest_processed_batch = self.loader.latest_processed_batch(&mut conn).await?;
        let last_processable_batch = self.loader.last_ready_to_be_loaded_batch(&mut conn).await?;
        drop(conn);
        anyhow::ensure!(
            l1_batch_number > latest_processed_batch,
            "Cannot handle an already processed batch #{} (latest is #{})",
            l1_batch_number,
            latest_processed_batch
        );
        anyhow::ensure!(
            l1_batch_number <= last_processable_batch,
            "Cannot handle batch #{} as it is too far away from latest batch #{} (last processable batch is #{})",
            l1_batch_number,
            latest_processed_batch,
            last_processable_batch
        );

        let handler = self.factory.create_handler(l1_batch_number).await?;
        let (sender, receiver) = oneshot::channel();
        self.state.insert(l1_batch_number, receiver);
        Ok(Box::new(AsyncOutputHandler {
            internal: Some(OutputHandlerState::Running { handler, sender }),
        }))
    }
}

enum OutputHandlerState {
    Running {
        handler: Box<dyn StateKeeperOutputHandler>,
        sender: oneshot::Sender<BoxFuture<'static, anyhow::Result<()>>>,
    },
    Finished,
}

impl Debug for OutputHandlerState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputHandlerState").finish()
    }
}

#[derive(Debug)]
struct AsyncOutputHandler {
    internal: Option<OutputHandlerState>,
}

#[async_trait]
impl StateKeeperOutputHandler for AsyncOutputHandler {
    async fn handle_l2_block(&mut self, updates_manager: &UpdatesManager) -> anyhow::Result<()> {
        match &mut self.internal {
            Some(OutputHandlerState::Running { handler, .. }) => {
                handler.handle_l2_block(updates_manager).await
            }
            Some(OutputHandlerState::Finished) => {
                Err(anyhow::anyhow!("Cannot handle any more L2 blocks"))
            }
            None => Err(anyhow::anyhow!(
                "Unexpected state, missing output handler state"
            )),
        }
    }

    async fn handle_l1_batch(
        &mut self,
        updates_manager: Arc<UpdatesManager>,
    ) -> anyhow::Result<()> {
        let state = self.internal.take();
        match state {
            Some(OutputHandlerState::Running {
                mut handler,
                sender,
            }) => {
                self.internal = Some(OutputHandlerState::Finished);
                sender
                    .send(Box::pin(async move {
                        handler.handle_l1_batch(updates_manager).await
                    }))
                    .ok();
                Ok(())
            }
            Some(OutputHandlerState::Finished) => {
                self.internal = state;
                Err(anyhow::anyhow!("Cannot handle any more L1 batches"))
            }
            None => Err(anyhow::anyhow!(
                "Unexpected state, missing output handler state"
            )),
        }
    }
}

pub struct ConcurrentOutputHandlerFactoryTask<L: VmRunnerStorageLoader> {
    pool: ConnectionPool<Core>,
    loader: L,
    state: Arc<DashMap<L1BatchNumber, oneshot::Receiver<BoxFuture<'static, anyhow::Result<()>>>>>,
}

impl<L: VmRunnerStorageLoader> ConcurrentOutputHandlerFactoryTask<L> {
    pub async fn run(self, stop_receiver: watch::Receiver<bool>) -> anyhow::Result<()> {
        const SLEEP_INTERVAL: Duration = Duration::from_millis(50);

        let mut conn = self.pool.connection_tagged(L::name()).await?;
        let mut latest_processed_batch = self.loader.latest_processed_batch(&mut conn).await?;
        drop(conn);
        loop {
            if *stop_receiver.borrow() {
                tracing::info!("`ConcurrentOutputHandlerFactoryTask` was interrupted");
                return Ok(());
            }
            match self.state.remove(&(latest_processed_batch + 1)) {
                None => {
                    tracing::debug!(
                        "Output handler for batch #{} has not been created yet",
                        latest_processed_batch + 1
                    );
                    tokio::time::sleep(SLEEP_INTERVAL).await;
                }
                Some((_, receiver)) => {
                    // Wait until the future is sent through the receiver, happens when
                    // `handle_l1_batch` is called on the corresponding output handler
                    let future = receiver.await?;
                    // Wait until the future is completed, meaning that the `handle_l1_batch`
                    // computation has finished, and we can consider this batch to be completed
                    future.await?;
                    latest_processed_batch += 1;
                    let mut conn = self.pool.connection_tagged(L::name()).await?;
                    self.loader
                        .mark_l1_batch_as_completed(&mut conn, latest_processed_batch)
                        .await?;
                    drop(conn);
                }
            }
        }
    }
}