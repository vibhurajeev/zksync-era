use zksync_eth_client::L1EthBoundInterface;
use zksync_web3_decl::client::{DynClient, L1, L2};

use crate::resource::Resource;

/// A resource that provides L1 interface object to the service.
#[derive(Debug, Clone)]
pub struct EthInterfaceResource(pub Box<DynClient<L1>>);

impl Resource for EthInterfaceResource {
    fn name() -> String {
        "common/eth_interface".into()
    }
}

/// A resource that provides L2 interface object to the service.
/// It is expected to have the same URL as the `EthInterfaceResource`, but have different capabilities.
///
/// This resource is provided separately from `EthInterfaceResource`, to provide type safety in places, where the
/// component must work with L1-interface only and should use `EthInterfaceResource` instead.
#[derive(Debug, Clone)]
pub struct L2InterfaceResource(pub Box<DynClient<L2>>);

impl Resource for L2InterfaceResource {
    fn name() -> String {
        "common/l2_interface".into()
    }
}

/// A resource that provides L1 interface with signing capabilities to the service.
#[derive(Debug, Clone)]
pub struct BoundEthInterfaceResource(pub Box<L1EthBoundInterface>);

impl Resource for BoundEthInterfaceResource {
    fn name() -> String {
        "common/bound_eth_interface".into()
    }
}

/// Same as `BoundEthInterfaceResource`, but for managing EIP-4844 blobs.
#[derive(Debug, Clone)]
pub struct BoundEthInterfaceForBlobsResource(pub Box<L1EthBoundInterface>);

impl Resource for BoundEthInterfaceForBlobsResource {
    fn name() -> String {
        "common/bound_eth_interface_for_blobs".into()
    }
}
