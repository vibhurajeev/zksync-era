use std::convert::Infallible;
use std::{
    collections::HashMap,
    //borrow::Borrow,
    sync::Arc,
    time::Duration,
};

//Server related
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use warp::{http::StatusCode, Filter};
use zksync_config::ApiConfig;
use zksync_dal::{ConnectionPool, Core, CoreDal};
use zksync_object_store::{bincode, ObjectStore};
use zksync_prover_interface::outputs::L1BatchProofForL1;
use zksync_types::{
    commitment::L1BatchWithMetadata,
    protocol_version::{ProtocolSemanticVersion, VersionPatch},
    L1BatchNumber, ProtocolVersionId,
};

//use zksync_prover_interface::outputs::L1BatchProofForL1;
//use zksync_types::protocol_version::{L1VerifierConfig, ProtocolSemanticVersion};
use crate::{
    implementations::resources::{
        object_store::ObjectStoreResource,
        pools::{MasterPool, PoolResource},
    },
    FromContext, IntoContext, StopReceiver, Task, TaskId, WiringError, WiringLayer,
};

#[derive(Debug, Clone)]
pub struct MockStruct {
    blob_store: Arc<dyn ObjectStore>,
    pool: ConnectionPool<Core>,
    port: u16,
    // l1_verifier_config: L1VerifierConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProofWithL1BatchMetaData {
    bytes: Vec<u8>,
    metadata: L1BatchWithMetadata,
}

pub struct MockStructLayer(pub u16);

impl MockStructLayer {
    pub fn new(api_configs: Option<ApiConfig>) -> Self {
        let proof_api_port = match api_configs {
            Some(i) => i.web3_json_rpc.http_port - 20,
            None => 3030,
        };

        Self(proof_api_port)
    }
}

// impl Default for MockStructLayer {
//     fn default() -> Self {
//         Self::new(3030)
//     }
// }

impl MockStruct {
    pub fn new(
        blob_store: Arc<dyn ObjectStore>,
        pool: ConnectionPool<Core>,
        port: u16,
        //  l1_verifier_config: L1VerifierConfig,
    ) -> Self {
        Self {
            blob_store,
            pool,
            port,
            //  l1_verifier_config,
        }
    }

    pub async fn get_metadata(
        &self,
        batch_number: u32,
    ) -> Result<Option<ProofWithL1BatchMetaData>, String> {
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

        let protocol_version = ProtocolSemanticVersion {
            minor: ProtocolVersionId::Version24,
            patch: VersionPatch(2),
        };

        let proof = self
            .blob_store
            .get::<L1BatchProofForL1>((L1BatchNumber(batch_number), protocol_version))
            .await
            .map_err(|e| e.to_string())
            .unwrap();

        let serialized_proof = bincode::serialize(&proof).unwrap();

        let final_proof = ProofWithL1BatchMetaData {
            bytes: serialized_proof,
            metadata: metadata.unwrap(),
        };

        Ok(Some(final_proof))
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
            mock: MockStruct::new(input.object_store.0, master_pool, self.0),
        })
    }
}

#[async_trait::async_trait]
impl Task for MockStruct {
    fn id(&self) -> TaskId {
        "proof_api_layer".into()
    }

    async fn run(self: Box<Self>, stop_receiver: StopReceiver) -> anyhow::Result<()> {
        let port = self.port;
        let proof_with_metadata_api = warp::path("metadata")
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
        let (_, server) = warp::serve(proof_with_metadata_api).bind_with_graceful_shutdown(
            ([127, 0, 0, 1], port),
            async move {
                loop {
                    // Check if the stop signal has been received
                    if *stop_receiver.0.borrow() {
                        break;
                    }
                    sleep(Duration::from_secs(1)).await;
                }
            },
        );

        println!("API server is running on http://127.0.0.1:{}", port);

        // Await the server to run until the stop signal is received
        server.await;

        Ok(())
    }
}
