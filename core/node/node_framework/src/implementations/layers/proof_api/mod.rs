use std::{
    //borrow::Borrow,
    //sync::Arc,
    collections::HashMap,
    time::Duration,
};

use tokio::time::sleep;
use zksync_dal::{ConnectionPool, Core, CoreDal};
use zksync_types::{commitment::L1BatchWithMetadata, L1BatchNumber};

//use zksync_prover_interface::outputs::L1BatchProofForL1;
//use zksync_types::protocol_version::{L1VerifierConfig, ProtocolSemanticVersion};
use crate::{
    implementations::resources::{
        object_store::ObjectStoreResource,
        pools::{MasterPool, PoolResource},
    },
    FromContext, IntoContext, StopReceiver, Task, TaskId, WiringError, WiringLayer,
};

//Server related
use serde::Serialize;
use std::convert::Infallible;
use warp::http::StatusCode;
use warp::Filter;

#[derive(Debug, Clone)]
pub struct MockStruct {
    //blob_store: Arc<dyn ObjectStore>,
    pool: ConnectionPool<Core>,
    // l1_verifier_config: L1VerifierConfig,
}

pub struct MockStructLayer();

impl MockStructLayer {
    pub fn new() -> Self {
        Self()
    }
}

impl Default for MockStructLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl MockStruct {
    pub fn new(
        //blob_store: Arc<dyn ObjectStore>,
        pool: ConnectionPool<Core>,
        //  l1_verifier_config: L1VerifierConfig,
    ) -> Self {
        Self {
            //  blob_store,
            pool,
            //  l1_verifier_config,
        }
    }

    pub async fn get_metadata(
        &self,
        batch_number: u32,
    ) -> Result<Option<L1BatchWithMetadata>, String> {
        let mut storage = self
            .pool
            .connection_tagged("proof_api")
            .await
            .map_err(|e| e.to_string())?;

        let metadata = storage
            .blocks_dal()
            .get_l1_batch_metadata(L1BatchNumber(batch_number))
            .await
            .map_err(|e| e.to_string())?;

        Ok(metadata)
    }
}

#[derive(Debug, FromContext)]
#[context(crate = crate)]
pub struct Input {
    pub object_store: ObjectStoreResource,
    pub master_pool: PoolResource<MasterPool>,
}

#[derive(Debug, IntoContext)]
#[context(crate = crate)]
pub struct Output {
    #[context(task)]
    pub mock: MockStruct,
}

#[async_trait::async_trait]
impl WiringLayer for MockStructLayer {
    type Input = Input;
    type Output = Output;

    fn layer_name(&self) -> &'static str {
        "proof_api_layer"
    }

    async fn wire(self, input: Self::Input) -> Result<Self::Output, WiringError> {
        println!("\n\nACTUALLL WIRING {:#?}\n\n", input);

        let master_pool = input.master_pool.get().await.unwrap();

        Ok(Output {
            mock: MockStruct::new(
                //input.object_store.0,
                master_pool,
            ),
        })
    }
}

#[async_trait::async_trait]
impl Task for MockStruct {
    fn id(&self) -> TaskId {
        "proof_api_layer".into()
    }

    async fn run(self: Box<Self>, stop_receiver: StopReceiver) -> anyhow::Result<()> {
        let api = warp::path("metadata")
            .and(warp::query::<std::collections::HashMap<String, String>>())
            .and(warp::any().map(move || self.clone()))
            .and_then(
                |params: HashMap<String, String>, mock_struct: Box<MockStruct>| async move {
                    let l1_batch_number = params
                        .get("l1BatchNumber")
                        .and_then(|n| n.parse::<u32>().ok())
                        .unwrap_or(0);

                    let metadata = mock_struct
                        .get_metadata(l1_batch_number)
                        .await
                        .unwrap_or_else(|_| None);

                    if let Some(metadata) = metadata {
                        Ok::<warp::reply::WithStatus<warp::reply::Json>, Infallible>(
                            warp::reply::with_status(warp::reply::json(&metadata), StatusCode::OK)
                                as warp::reply::WithStatus<warp::reply::Json>,
                        )
                    } else {
                        Ok(warp::reply::with_status(
                            warp::reply::json(&metadata),
                            StatusCode::NOT_FOUND,
                        )
                            as warp::reply::WithStatus<warp::reply::Json>)
                    }
                },
            );

        // Start the API server on a fixed port
        let (_, server) =
            warp::serve(api).bind_with_graceful_shutdown(([127, 0, 0, 1], 3030), async move {
                loop {
                    // Check if the stop signal has been received
                    if *stop_receiver.0.borrow() {
                        break;
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            });

        println!("API server is running on http://127.0.0.1:3030");

        // Await the server to run until the stop signal is received
        server.await;

        Ok(())
    }
}
