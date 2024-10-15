use std::convert::Infallible;
use std::{
    collections::HashMap,
    //borrow::Borrow,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
//Server related
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use warp::{http::StatusCode, Filter};
use zksync_config::ApiConfig;
use zksync_dal::{ConnectionPool, Core, CoreDal};
use zksync_l1_contract_interface::i_executor::commit::kzg::{
    pubdata_to_blob_commitments, KzgInfo, ZK_SYNC_BYTES_PER_BLOB,
};
use zksync_object_store::{bincode, ObjectStore};
use zksync_prover_interface::outputs::L1BatchProofForL1;
use zksync_types::{
    blob::num_blobs_required,
    commitment::{CommitmentCommonInput, CommitmentInput, L1BatchWithMetadata},
    eth_sender::SidecarBlobV1,
    ethabi::Token,
    protocol_version::{ProtocolSemanticVersion, VersionPatch},
    writes::{InitialStorageWrite, RepeatedStorageWrite, StateDiffRecord},
    L1BatchNumber, ProtocolVersionId, StorageKey, H256, U256,
};

use crypto_codegen::serialize_proof;
use zksync_utils::h256_to_u256;

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

const PUBDATA_SOURCE_BLOBS: u8 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProofWithL1BatchMetaData {
    bytes: Token,
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
    ) -> Result<Option<(ProofWithL1BatchMetaData, Vec<H256>, Vec<u8>, Vec<[u8; 32]>)>, String> {
        let l1_batch_number: L1BatchNumber = L1BatchNumber(batch_number);
        let mut storage = self
            .pool
            .connection_tagged("proof_api")
            .await
            .map_err(|e| e.to_string())?;

        let metadata = storage
            .blocks_dal()
            .get_l1_batch_metadata(l1_batch_number)
            .await
            .map_err(|e| e.to_string())?
            .unwrap();

        // TODO(PLA-731): ensure that the protocol version is always available.
        let protocol_version = metadata
            .header
            .protocol_version
            .unwrap_or_else(ProtocolVersionId::last_potentially_undefined);

        let protocol_semantic_version = ProtocolSemanticVersion {
            minor: protocol_version,
            //TODO: Check if below patch requires manual updates.
            patch: VersionPatch(2),
        };

        let (pubdata_commitments, blob_commitments, versioned_hashes) = if protocol_version
            .is_post_1_4_2()
        {
            let pubdata_input: Vec<u8> = metadata
                .header
                .pubdata_input
                .clone()
                .with_context(|| {
                    format!("`pubdata_input` is missing for L1 batch #{l1_batch_number}")
                })
                .map_err(|e| e.to_string())?;

            let pubdata_commitments =
                pubdata_input
                    .chunks(ZK_SYNC_BYTES_PER_BLOB)
                    .flat_map(|blob| {
                        let kzg_info = KzgInfo::new(blob);
                        kzg_info.to_pubdata_commitment()
                    });

            let versioned_hashes = metadata
                .header
                .pubdata_input
                .clone()
                .unwrap()
                .chunks(ZK_SYNC_BYTES_PER_BLOB)
                .map(|blob| {
                    let kzg_info = KzgInfo::new(blob);
                    kzg_info.versioned_hash
                })
                .collect::<Vec<[u8; 32]>>();

            (
                std::iter::once(PUBDATA_SOURCE_BLOBS)
                    .chain(pubdata_commitments.clone())
                    .collect::<Vec<_>>(),
                pubdata_to_blob_commitments(num_blobs_required(&protocol_version), &pubdata_input),
                versioned_hashes,
            )
        } else {
            (
                vec![0u8; ZK_SYNC_BYTES_PER_BLOB],
                vec![H256::zero(); num_blobs_required(&protocol_version)],
                vec![[0u8; 32]],
            )
        };

        // let proof = self
        //     .blob_store
        //     .get::<L1BatchProofForL1>((L1BatchNumber(batch_number), protocol_semantic_version))
        //     .await
        //     .map_err(|e| e.to_string())
        //     .unwrap();

        let proof = match self
            .blob_store
            .get::<L1BatchProofForL1>((L1BatchNumber(batch_number), protocol_semantic_version))
            .await
        {
            Ok(proof) => Some(proof),
            Err(e) => {
                println!("Proof not found: {}", e);
                None
            }
        };

        let proof_input = match proof {
            Some(ref p) if metadata.header.protocol_version.unwrap().is_pre_boojum() => {
                let aggregation_result_coords = Token::Array(
                    p.aggregation_result_coords
                        .iter()
                        .map(|bytes| Token::Uint(U256::from_big_endian(bytes)))
                        .collect(),
                );
        
                let (_, serialized_proof) = serialize_proof(&p.scheduler_proof);
        
                Token::Tuple(vec![
                    aggregation_result_coords,
                    Token::Array(serialized_proof.into_iter().map(Token::Uint).collect()),
                ])
            },
            _ => Token::Array(Vec::new()),
        };

        let final_proof = ProofWithL1BatchMetaData {
            bytes: proof_input,
            metadata: metadata,
        };

        Ok(Some((
            final_proof,
            blob_commitments,
            pubdata_commitments,
            versioned_hashes,
        )))
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
