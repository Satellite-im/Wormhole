use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::channel::mpsc::channel;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt, TryStreamExt};
use futures_timeout::TimeoutExt;
use indexmap::IndexSet;
use ipfs::p2p::{
    IdentifyConfiguration, KadConfig, KadInserts, MultiaddrExt, PubsubConfig, TransportConfig,
};
use ipfs::{DhtMode, Ipfs, Keypair, Protocol, UninitializedIpfs};
use parking_lot::RwLock;
use rust_ipfs as ipfs;
use rust_ipfs::p2p::{RequestResponseConfig, UpgradeVersion};
use std::any::Any;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use store::protocols;
#[cfg(not(target_arch = "wasm32"))]
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{Instrument, Span};
use uuid::Uuid;
use warp::raygun::community::{CommunityRole, RoleId};

use crate::config::{Bootstrap, DiscoveryType};
use crate::store::discovery::Discovery;
use crate::store::phonebook::PhoneBook;
use crate::store::{ecdh_decrypt, PeerIdExt};
use crate::store::{MAX_IMAGE_SIZE, MAX_USERNAME_LENGTH, MIN_USERNAME_LENGTH};
use crate::utils::{ByteCollection, ReaderStream};
use config::Config;
use store::document::ResolvedRootDocument;
use store::event_subscription::EventSubscription;
use store::files::FileStore;
use store::identity::IdentityStore;
use store::message::MessageStore;
use utils::ExtensionType;
use warp::constellation::directory::Directory;
use warp::constellation::file::FileType;
use warp::constellation::{
    Constellation, ConstellationEvent, ConstellationEventKind, ConstellationEventStream,
    ConstellationProgressStream,
};
use warp::crypto::keypair::PhraseType;
use warp::crypto::zeroize::Zeroizing;
use warp::crypto::{KeyMaterial, DID};
use warp::error::Error;
use warp::module::Module;
use warp::multipass::identity::{
    FriendRequest, Identifier, Identity, IdentityImage, IdentityProfile, IdentityUpdate,
    Relationship,
};
use warp::multipass::{
    identity, Friends, GetIdentity, IdentityImportOption, IdentityInformation, ImportLocation,
    LocalIdentity, MultiPass, MultiPassEvent, MultiPassEventKind, MultiPassEventStream,
    MultiPassImportExport,
};
use warp::raygun::{
    community::{
        Community, CommunityChannel, CommunityChannelType, CommunityInvite, RayGunCommunity,
    },
    AttachmentEventStream, Conversation, ConversationImage, EmbedState, GroupPermissionOpt,
    Location, Message, MessageEvent, MessageEventStream, MessageOptions, MessageReference,
    MessageStatus, Messages, PinState, RayGun, RayGunAttachment, RayGunConversationInformation,
    RayGunEventKind, RayGunEventStream, RayGunEvents, RayGunGroupConversation, RayGunStream,
    ReactionState,
};
use warp::tesseract::{Tesseract, TesseractEvent};
use warp::warp::Warp;
use warp::{Extension, SingleHandle};

mod behaviour;
pub mod config;
pub mod shuttle;
pub mod store;
mod thumbnail;
mod utils;

const PUBSUB_MAX_BUF: usize = 8_388_608;

#[derive(Clone)]
pub struct WarpIpfs {
    tesseract: Tesseract,
    inner: Arc<Inner>,
    multipass_tx: EventSubscription<MultiPassEventKind>,
    raygun_tx: EventSubscription<RayGunEventKind>,
    constellation_tx: EventSubscription<ConstellationEventKind>,
}

pub type WarpIpfsInstance = Warp<WarpIpfs, WarpIpfs, WarpIpfs>;

struct Inner {
    config: Config,
    identity_guard: tokio::sync::Mutex<()>,
    init_guard: tokio::sync::Mutex<()>,
    span: RwLock<Span>,
    components: RwLock<Option<Components>>,
}

// Holds the initialized components
#[derive(Clone)]
struct Components {
    ipfs: Ipfs,
    identity_store: IdentityStore,
    message_store: MessageStore,
    file_store: FileStore,
}

#[derive(Default)]
pub struct WarpIpfsBuilder {
    config: Config,
    // use_multipass: bool,
    // use_raygun: bool,
    // use_constellation: bool,
    tesseract: Option<Tesseract>,
}

impl WarpIpfsBuilder {
    pub fn set_config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    pub fn set_tesseract(mut self, tesseract: Tesseract) -> Self {
        self.tesseract = Some(tesseract);
        self
    }
}

impl core::future::IntoFuture for WarpIpfsBuilder {
    type IntoFuture = BoxFuture<'static, Self::Output>;
    type Output = WarpIpfsInstance;

    fn into_future(self) -> Self::IntoFuture {
        async move { WarpIpfs::new(self.config, self.tesseract).await }.boxed()
    }
}

impl WarpIpfs {
    pub async fn new(config: Config, tesseract: impl Into<Option<Tesseract>>) -> WarpIpfsInstance {
        let multipass_tx = EventSubscription::new();
        let raygun_tx = EventSubscription::new();
        let constellation_tx = EventSubscription::new();
        let span = RwLock::new(Span::current());

        let tesseract = match tesseract.into() {
            Some(tesseract) => tesseract,
            None if !config.persist() => Tesseract::default(),
            None => {
                #[cfg(target_arch = "wasm32")]
                {
                    let tesseract = Tesseract::default();
                    _ = tesseract.load_from_storage();
                    tesseract.set_autosave();
                    tesseract
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    // Note: We could probably assert here since a path should be supplied when is it persist,
                    //       but for now we will create a default tesseract if Config::path is `None`
                    match config.path() {
                        Some(path) => {
                            Tesseract::open_or_create(path, "tesseract.bin").unwrap_or_default()
                        }
                        None => Tesseract::default(),
                    }
                }
            }
        };

        let inner = Arc::new(Inner {
            config,
            components: Default::default(),
            identity_guard: Default::default(),
            init_guard: Default::default(),
            span,
        });

        let identity = WarpIpfs {
            tesseract,
            inner,
            multipass_tx,
            raygun_tx,
            constellation_tx,
        };

        if !identity.tesseract.is_unlock() {
            let inner = identity.clone();
            async_rt::task::dispatch(async move {
                let mut stream = inner.tesseract.subscribe();
                while let Some(event) = stream.next().await {
                    if matches!(event, TesseractEvent::Unlocked) {
                        break;
                    }
                }
                let _ = inner.initialize_store(false).await;
            });
        } else {
            let _ = identity.initialize_store(false).await;
        }

        Warp::new(&identity, &identity, &identity)
    }

    async fn initialize_store(&self, init: bool) -> Result<(), Error> {
        let tesseract = self.tesseract.clone();

        if init && self.inner.components.read().is_some() {
            tracing::warn!("Identity is already loaded");
            return Err(Error::IdentityExist);
        }

        let keypair = match (init, tesseract.exist("keypair")) {
            (true, false) => {
                tracing::info!("Keypair doesnt exist. Generating keypair....");
                let kp = Keypair::generate_ed25519()
                    .try_into_ed25519()
                    .map_err(|e| {
                        tracing::error!(error = %e, "Unreachable. Report this as a bug");
                        Error::Other
                    })?;
                let encoded_kp = bs58::encode(&kp.to_bytes()).into_string();
                tesseract.set("keypair", &encoded_kp)?;
                let bytes = Zeroizing::new(kp.secret().as_ref().to_vec());
                Keypair::ed25519_from_bytes(bytes).map_err(|_| Error::PrivateKeyInvalid)?
            }
            (false, true) | (true, true) => {
                tracing::info!("Fetching keypair from tesseract");
                let keypair = tesseract.retrieve("keypair")?;
                let kp = Zeroizing::new(bs58::decode(keypair).into_vec()?);
                let id_kp = warp::crypto::ed25519_dalek::Keypair::from_bytes(&kp)?;
                Keypair::ed25519_from_bytes(id_kp.secret.to_bytes())
                    .map_err(|_| Error::PrivateKeyInvalid)?
            }
            _ => return Err(Error::OtherWithContext("Unable to initialize store".into())),
        };

        self.init_ipfs(keypair).await?;

        Ok(())
    }

    pub(crate) async fn init_ipfs(&self, keypair: Keypair) -> Result<(), Error> {
        // Since some trait functions are not async (this may change in the future), we cannot hold a lock that is not async-aware
        // through this function without suffering dead locks in the process through await points with the lock held.
        // As a result, we holds a tokio mutex lock here to allow a single call to this function on initializing
        // preventing any other calls until this lock drops, in which case may return an error if the components
        // been successfully initialized
        let _g = self.inner.init_guard.lock().await;

        // We check again in case
        // - the initialization was previously successful (after tesseract was unlocked)
        // - creating the account was previously successful
        // - importing the account was successful (assuming the formers were not successful)
        if self.inner.components.read().is_some() {
            return Err(Error::IdentityExist);
        }

        let peer_id = keypair.public().to_peer_id();

        let did = peer_id.to_did().expect("Valid conversion");

        tracing::info!(peer_id = %peer_id, did = %did);

        let span = tracing::trace_span!(parent: &Span::current(), "warp-ipfs", identity = %did);

        *self.inner.span.write() = span.clone();

        let _g = span.enter();

        let empty_bootstrap = match &self.inner.config.bootstrap() {
            Bootstrap::Ipfs => false,
            Bootstrap::Custom(addr) => addr.is_empty(),
            Bootstrap::None => true,
        };

        if empty_bootstrap && !self.inner.config.ipfs_setting().dht_client {
            tracing::warn!(
                "Bootstrap list is empty. Will not be able to perform a bootstrap for DHT"
            );
        }

        let (pb_tx, pb_rx) = channel(50);

        let behaviour = behaviour::Behaviour {
            phonebook: behaviour::phonebook::Behaviour::new(self.multipass_tx.clone(), pb_rx),
        };

        let mut request_response_configs = vec![
            RequestResponseConfig {
                protocol: protocols::EXCHANGE_PROTOCOL.as_ref().into(),
                max_request_size: 8 * 1024,
                max_response_size: 16 * 1024,
                ..Default::default()
            },
            RequestResponseConfig {
                protocol: protocols::IDENTITY_PROTOCOL.as_ref().into(),
                max_request_size: 256 * 1024,
                max_response_size: 512 * 1024,
                ..Default::default()
            },
            RequestResponseConfig {
                protocol: protocols::DISCOVERY_PROTOCOL.as_ref().into(),
                max_request_size: 256 * 1024,
                max_response_size: 512 * 1024,
                ..Default::default()
            },
        ];

        if let config::Discovery::Shuttle { .. } = &self.inner.config.store_setting().discovery {
            request_response_configs.extend([
                RequestResponseConfig {
                    protocol: protocols::SHUTTLE_IDENTITY.as_ref().into(),
                    max_request_size: 256 * 1024,
                    max_response_size: 512 * 1024,
                    ..Default::default()
                },
                RequestResponseConfig {
                    protocol: protocols::SHUTTLE_MESSAGE.as_ref().into(),
                    max_request_size: 256 * 1024,
                    max_response_size: 512 * 1024,
                    ..Default::default()
                },
            ]);
        }

        tracing::info!("Starting ipfs");
        let mut uninitialized = UninitializedIpfs::new()
            .with_identify({
                let mut idconfig = IdentifyConfiguration {
                    protocol_version: "/satellite/warp/0.1".into(),
                    ..Default::default()
                };
                if let Some(agent) = self.inner.config.ipfs_setting().agent_version.as_ref() {
                    idconfig.agent_version.clone_from(agent);
                }
                idconfig
            })
            .with_bitswap()
            .with_ping(Default::default())
            .with_pubsub(PubsubConfig {
                max_transmit_size: PUBSUB_MAX_BUF,
                ..Default::default()
            })
            .with_relay(true)
            .with_request_response(request_response_configs)
            .set_listening_addrs(self.inner.config.listen_on().to_vec())
            .with_custom_behaviour(behaviour)
            .set_keypair(&keypair)
            .set_span(span.clone())
            .set_transport_configuration(TransportConfig {
                enable_memory_transport: self.inner.config.ipfs_setting().memory_transport,
                // We check the target arch since it doesnt really make much sense to have each native peer to use websocket or webrtc transport
                // as such connections would be established through the relay
                enable_websocket: cfg!(target_arch = "wasm32"),
                version: UpgradeVersion::Standard,
                ..Default::default()
            });

        // TODO: Uncomment for persistence on wasm once config option is added
        #[cfg(target_arch = "wasm32")]
        {
            if self.inner.config.persist() {
                // Namespace will used the public key to prevent conflicts between multiple instances during testing.
                uninitialized = uninitialized.set_storage_type(rust_ipfs::StorageType::IndexedDb {
                    namespace: Some(keypair.public().to_peer_id().to_string()),
                });
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(path) = self.inner.config.path() {
                tracing::info!("Instance will be persistent");
                tracing::info!("Path set: {}", path.display());
                uninitialized = uninitialized.set_path(path);
            }
        }

        if matches!(
            self.inner.config.store_setting().discovery,
            config::Discovery::Namespace {
                discovery_type: DiscoveryType::DHT,
                ..
            }
        ) {
            uninitialized = uninitialized.with_kademlia(
                either::Either::Left(KadConfig {
                    query_timeout: std::time::Duration::from_secs(60),
                    publication_interval: Some(Duration::from_secs(30 * 60)),
                    provider_record_ttl: Some(Duration::from_secs(60 * 60)),
                    insert_method: KadInserts::Manual,
                    ..Default::default()
                }),
                Default::default(),
            );
        }

        if matches!(
            self.inner.config.store_setting().discovery,
            config::Discovery::Namespace {
                discovery_type: DiscoveryType::RzPoint { .. },
                ..
            }
        ) {
            uninitialized = uninitialized.with_rendezvous_client();
        }

        // If memory transport is enabled, this means that test are running and in such case, we should have the addresses be
        // treated as external addresses.
        if self.inner.config.ipfs_setting().memory_transport {
            uninitialized = uninitialized.listen_as_external_addr();
        }

        #[cfg(not(target_arch = "wasm32"))]
        if self.inner.config.ipfs_setting().portmapping {
            uninitialized = uninitialized.with_upnp();
        }

        #[cfg(not(target_arch = "wasm32"))]
        if self.inner.config.ipfs_setting().mdns.enable {
            uninitialized = uninitialized.with_mdns();
        }

        let ipfs = uninitialized.start().await?;

        if self.inner.config.enable_relay() {
            let mut relay_peers = HashSet::new();

            for mut addr in self
                .inner
                .config
                .ipfs_setting()
                .relay_client
                .relay_address
                .iter()
                .chain(self.inner.config.bootstrap().address().iter())
                .cloned()
            {
                if addr.is_relayed() {
                    tracing::warn!("Relay circuits cannot be used as relays");
                    continue;
                }

                let Some(peer_id) = addr.extract_peer_id() else {
                    tracing::warn!("{addr} does not contain a peer id. Skipping");
                    continue;
                };

                if let Err(e) = ipfs.add_peer((peer_id, addr.clone())).await {
                    tracing::warn!("Failed to add relay to address book: {e}");
                }

                if let Err(e) = ipfs.add_relay(peer_id, addr).await {
                    tracing::error!("Error adding relay: {e}");
                    continue;
                }

                relay_peers.insert(peer_id);
            }

            if relay_peers.is_empty() {
                tracing::warn!("No relays available");
            }

            // Use the selected relays
            let relay_connection_task = {
                let ipfs = ipfs.clone();
                let quorum = self.inner.config.ipfs_setting().relay_client.quorum;
                async move {
                    let mut counter = 0;
                    for relay_peer in relay_peers {
                        match ipfs
                            .enable_relay(Some(relay_peer))
                            .timeout(Duration::from_secs(5))
                            .await
                        {
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => {
                                tracing::error!("Failed to use {relay_peer} as a relay: {e}");
                                continue;
                            }
                            Err(_) => {
                                tracing::error!("Relay connection timed out");
                                continue;
                            }
                        };

                        match quorum {
                            config::RelayQuorum::First => break,
                            config::RelayQuorum::N(n) => {
                                if counter < n {
                                    counter += 1;
                                    continue;
                                }
                                break;
                            }
                            config::RelayQuorum::All => continue,
                        }
                    }

                    let list = ipfs.list_relays(true).await.unwrap_or_default();
                    for addr in list.iter().flat_map(|(_, addrs)| addrs) {
                        tracing::info!("Listening on {}", addr.clone().with(Protocol::P2pCircuit));
                    }
                }
            };

            if self.inner.config.ipfs_setting().relay_client.background {
                async_rt::task::dispatch(relay_connection_task);
            } else {
                relay_connection_task.await;
            }
        }

        if let config::Discovery::Shuttle { addresses } =
            self.inner.config.store_setting().discovery.clone()
        {
            let mut nodes = IndexSet::new();
            for mut addr in addresses {
                let Some(peer_id) = addr.extract_peer_id() else {
                    tracing::warn!("{addr} does not contain a peer id. Skipping");
                    continue;
                };

                if let Err(_e) = ipfs.add_peer((peer_id, addr)).await {
                    // TODO:
                    continue;
                }

                nodes.insert(peer_id);
            }

            for node in nodes {
                if let Err(_e) = ipfs.connect(node).await {
                    // TODO
                }
            }
        }

        if matches!(
            self.inner.config.store_setting().discovery,
            config::Discovery::Namespace {
                discovery_type: DiscoveryType::DHT,
                ..
            }
        ) {
            match self.inner.config.bootstrap() {
                Bootstrap::Custom(addrs) => {
                    for addr in addrs {
                        if let Err(e) = ipfs.add_bootstrap(addr.clone()).await {
                            tracing::warn!(address = %addr, error = %e, "unable to add address to bootstrap");
                        }
                    }
                }
                Bootstrap::Ipfs => {
                    if let Err(e) = ipfs.default_bootstrap().await {
                        tracing::warn!(error = %e, "unable to add ipfs bootstrap");
                    }
                }
                _ => {}
            };

            if self.inner.config.ipfs_setting().dht_client {
                if let Err(e) = ipfs.dht_mode(DhtMode::Client).await {
                    tracing::warn!(error = %e, "unable to set dht to client mode");
                }
            }

            if !empty_bootstrap {
                async_rt::task::dispatch({
                    let ipfs = ipfs.clone();
                    async move {
                        loop {
                            if let Err(e) = ipfs.bootstrap().await {
                                tracing::error!("Failed to bootstrap: {e}")
                            }

                            futures_timer::Delay::new(Duration::from_secs(60 * 5)).await;
                        }
                    }
                });
            }
        }

        let relays = ipfs
            .list_relays(false)
            .await
            .unwrap_or_default()
            .iter()
            .flat_map(|(peer_id, addrs)| {
                addrs
                    .iter()
                    .map(|addr| {
                        addr.clone()
                            .with(Protocol::P2p(*peer_id))
                            .with(Protocol::P2pCircuit)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        if let config::Discovery::Namespace {
            discovery_type: DiscoveryType::RzPoint { addresses },
            ..
        } = &self.inner.config.store_setting().discovery
        {
            for mut addr in addresses.iter().cloned() {
                let Some(peer_id) = addr.extract_peer_id() else {
                    continue;
                };

                if let Err(e) = ipfs.add_peer((peer_id, addr)).await {
                    tracing::error!("Error adding peer to address book {e}");
                    continue;
                }

                if !ipfs.is_connected(peer_id).await.unwrap_or_default() {
                    let _ = ipfs.connect(peer_id).await;
                }
            }
        }

        let discovery =
            Discovery::new(&ipfs, &self.inner.config.store_setting().discovery, &relays);

        let phonebook = PhoneBook::new(discovery.clone(), pb_tx);

        tracing::info!("Initializing identity profile");
        let identity_store = IdentityStore::new(
            &ipfs,
            &self.inner.config,
            self.multipass_tx.clone(),
            &phonebook,
            &discovery,
            &span,
        )
        .await?;

        tracing::info!("Identity initialized");

        let root = identity_store.root_document();

        let filestore = FileStore::new(
            &ipfs,
            root,
            &self.inner.config,
            self.constellation_tx.clone(),
            &span,
        )
        .await;

        let message_store = MessageStore::new(
            &ipfs,
            discovery,
            &filestore,
            self.raygun_tx.clone(),
            &identity_store,
        )
        .await;

        tracing::info!("Messaging store initialized");

        *self.inner.components.write() = Some(Components {
            ipfs,
            identity_store,
            message_store,
            file_store: filestore,
        });

        // Announce identity out to mesh if identity has been created at that time
        if let Ok(store) = self.identity_store(true).await {
            let _ = store
                .announce_identity_to_mesh()
                .instrument(span.clone())
                .await;
        }
        Ok(())
    }

    pub(crate) async fn identity_store(&self, created: bool) -> Result<IdentityStore, Error> {
        if !self.tesseract.is_unlock() {
            return Err(Error::TesseractLocked);
        }
        if !self.tesseract.exist("keypair") {
            return Err(Error::IdentityNotCreated);
        }

        let store = self
            .inner
            .components
            .read()
            .as_ref()
            .map(|com| com.identity_store.clone())
            .ok_or(Error::MultiPassExtensionUnavailable)?;

        if created && !store.local_id_created().await {
            return Err(Error::IdentityNotCreated);
        }
        Ok(store)
    }

    pub(crate) fn messaging_store(&self) -> Result<MessageStore, Error> {
        self.inner
            .components
            .read()
            .as_ref()
            .map(|com| com.message_store.clone())
            .ok_or(Error::RayGunExtensionUnavailable)
    }

    pub(crate) fn file_store(&self) -> Result<FileStore, Error> {
        self.inner
            .components
            .read()
            .as_ref()
            .map(|com| com.file_store.clone())
            .ok_or(Error::ConstellationExtensionUnavailable)
    }

    pub(crate) fn direct_identity_store(&self) -> Result<IdentityStore, Error> {
        let store = self
            .inner
            .components
            .read()
            .as_ref()
            .map(|com| com.identity_store.clone())
            .ok_or(Error::MultiPassExtensionUnavailable)?;

        Ok(store)
    }

    pub(crate) fn ipfs(&self) -> Result<Ipfs, Error> {
        self.inner
            .components
            .read()
            .as_ref()
            .map(|com| com.ipfs.clone())
            .ok_or(Error::MultiPassExtensionUnavailable)
    }
}

impl Extension for WarpIpfs {
    fn id(&self) -> String {
        "warp-ipfs".to_string()
    }
    fn name(&self) -> String {
        "Warp Ipfs".into()
    }

    fn module(&self) -> Module {
        Module::Accounts
    }
}

impl SingleHandle for WarpIpfs {
    fn handle(&self) -> Result<Box<dyn Any>, Error> {
        self.ipfs().map(|ipfs| Box::new(ipfs) as Box<_>)
    }
}

#[async_trait::async_trait]
impl MultiPass for WarpIpfs {
    async fn create_identity(
        &mut self,
        username: Option<&str>,
        passphrase: Option<&str>,
    ) -> Result<IdentityProfile, Error> {
        let _g = self.inner.identity_guard.lock().await;

        tracing::info!(
            "create_identity with username: {username:?} and containing passphrase: {}",
            passphrase.is_some()
        );

        if self.inner.components.read().is_some() {
            tracing::info!("Store is initialized with existing identity");
            return Err(Error::IdentityExist);
        }

        if let Some(u) = username.map(|u| u.trim()) {
            let username_len = u.len();

            if !(MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&username_len) {
                return Err(Error::InvalidLength {
                    context: "username".into(),
                    current: username_len,
                    minimum: Some(MIN_USERNAME_LENGTH),
                    maximum: Some(MAX_USERNAME_LENGTH),
                });
            }
        }

        let (phrase, can_include) = match passphrase {
            Some(phrase) => {
                tracing::info!("Passphrase was supplied");
                (phrase.to_string(), false)
            }
            None => (
                warp::crypto::keypair::generate_mnemonic_phrase(PhraseType::Standard).into_phrase(),
                true,
            ),
        };

        let tesseract = self.tesseract.clone();
        if !tesseract.exist("keypair") {
            tracing::warn!("Loading keypair generated from mnemonic phrase into tesseract");
            warp::crypto::keypair::mnemonic_into_tesseract(
                &tesseract,
                &phrase,
                None,
                self.inner.config.save_phrase(),
                false,
            )?;
        }

        tracing::info!("Initializing stores");
        self.initialize_store(true).await?;
        tracing::info!("Stores initialized. Creating identity");
        let identity = self
            .identity_store(false)
            .await?
            .create_identity(username)
            .await?;
        tracing::info!("Identity with {} has been created", identity.did_key());
        let profile = IdentityProfile::new(identity, can_include.then_some(phrase));
        Ok(profile)
    }

    fn get_identity(&self, id: impl Into<Identifier>) -> GetIdentity {
        let store = match self.direct_identity_store() {
            Ok(store) => store,
            _ => return GetIdentity::new(id, stream::empty().boxed()),
        };

        store.lookup(id)
    }
}

#[async_trait::async_trait]
impl LocalIdentity for WarpIpfs {
    async fn identity(&self) -> Result<Identity, Error> {
        let store = self.identity_store(true).await?;
        store.own_identity().await
    }

    async fn profile_picture(&self) -> Result<IdentityImage, Error> {
        let store = self.identity_store(true).await?;
        store.profile_picture().await
    }

    async fn profile_banner(&self) -> Result<IdentityImage, Error> {
        let store = self.identity_store(true).await?;
        store.profile_banner().await
    }

    async fn update_identity(&mut self, option: IdentityUpdate) -> Result<(), Error> {
        let mut store = self.identity_store(true).await?;
        let mut identity = store.own_identity_document().await?;
        let ipfs = self.ipfs()?;

        let mut old_cid = None;
        enum OptType {
            Picture(Option<ExtensionType>),
            Banner(Option<ExtensionType>),
        }
        let (opt, stream) = match option {
            IdentityUpdate::Username(username) => {
                let len = username.chars().count();
                if !(4..=64).contains(&len) {
                    return Err(Error::InvalidLength {
                        context: "username".into(),
                        current: len,
                        minimum: Some(4),
                        maximum: Some(64),
                    });
                }

                identity.username = username;
                return store.identity_update(identity).await;
            }

            IdentityUpdate::ClearPicture => {
                let document = identity.metadata.profile_picture.take();
                if let Some(cid) = document {
                    if let Err(e) = store.delete_photo(cid).await {
                        tracing::error!("Error deleting picture: {e}");
                    }
                }
                return store.identity_update(identity).await;
            }
            IdentityUpdate::ClearBanner => {
                let document = identity.metadata.profile_banner.take();
                if let Some(cid) = document {
                    if let Err(e) = store.delete_photo(cid).await {
                        tracing::error!("Error deleting picture: {e}");
                    }
                }
                return store.identity_update(identity).await;
            }
            IdentityUpdate::StatusMessage(status) => {
                if let Some(status) = status.clone() {
                    let len = status.chars().count();
                    if len == 0 || len > 512 {
                        return Err(Error::InvalidLength {
                            context: "status".into(),
                            current: len,
                            minimum: Some(1),
                            maximum: Some(512),
                        });
                    }
                }
                identity.status_message = status;
                return store.identity_update(identity).await;
            }
            IdentityUpdate::ClearStatusMessage => {
                identity.status_message = None;
                return store.identity_update(identity).await;
            }
            IdentityUpdate::Picture(data) => {
                let len = data.len();
                if len == 0 || len > MAX_IMAGE_SIZE {
                    return Err(Error::InvalidLength {
                        context: "profile banner".into(),
                        current: len,
                        minimum: Some(1),
                        maximum: Some(MAX_IMAGE_SIZE),
                    });
                }
                let cursor = std::io::Cursor::new(data);

                let image = image::ImageReader::new(cursor).with_guessed_format()?;

                let format = image
                    .format()
                    .and_then(|format| ExtensionType::try_from(format).ok())
                    .unwrap_or(ExtensionType::Other);

                let inner = image.into_inner();

                let async_cursor = futures::io::Cursor::new(inner.into_inner());

                let stream =
                    ReaderStream::from_reader_with_cap(async_cursor, 512, Some(MAX_IMAGE_SIZE));
                (OptType::Picture(Some(format)), stream.boxed())
            }
            #[cfg(not(target_arch = "wasm32"))]
            IdentityUpdate::PicturePath(path) => {
                if !path.is_file() {
                    return Err(Error::IoError(std::io::Error::from(
                        std::io::ErrorKind::NotFound,
                    )));
                }

                let file = tokio::fs::File::open(&path).await?;

                let metadata = file.metadata().await?;

                let len = metadata.len() as _;

                if len == 0 || len > MAX_IMAGE_SIZE {
                    return Err(Error::InvalidLength {
                        context: "profile picture".into(),
                        current: len,
                        minimum: Some(1),
                        maximum: Some(MAX_IMAGE_SIZE),
                    });
                }

                let extension = path
                    .extension()
                    .and_then(OsStr::to_str)
                    .map(ExtensionType::from)
                    .unwrap_or(ExtensionType::Other);

                tracing::trace!("image size = {}", len);

                let stream =
                    ReaderStream::from_reader_with_cap(file.compat(), 512, Some(MAX_IMAGE_SIZE));

                (OptType::Picture(Some(extension)), stream.boxed())
            }
            #[cfg(target_arch = "wasm32")]
            IdentityUpdate::PicturePath(_) => {
                return Err(Error::Unimplemented);
            }
            IdentityUpdate::PictureStream(stream) => {
                let stream = ReaderStream::from_reader_with_cap(
                    stream.into_async_read(),
                    512,
                    Some(MAX_IMAGE_SIZE),
                );
                (OptType::Picture(None), stream.boxed())
            }
            IdentityUpdate::Banner(data) => {
                let len = data.len();
                if len == 0 || len > MAX_IMAGE_SIZE {
                    return Err(Error::InvalidLength {
                        context: "profile banner".into(),
                        current: len,
                        minimum: Some(1),
                        maximum: Some(MAX_IMAGE_SIZE),
                    });
                }

                let cursor = std::io::Cursor::new(data);

                let image = image::ImageReader::new(cursor).with_guessed_format()?;

                let format = image
                    .format()
                    .and_then(|format| ExtensionType::try_from(format).ok())
                    .unwrap_or(ExtensionType::Other);

                let inner = image.into_inner();

                let async_cursor = futures::io::Cursor::new(inner.into_inner());

                let stream =
                    ReaderStream::from_reader_with_cap(async_cursor, 512, Some(MAX_IMAGE_SIZE));

                (OptType::Banner(Some(format)), stream.boxed())
            }
            #[cfg(not(target_arch = "wasm32"))]
            IdentityUpdate::BannerPath(path) => {
                if !path.is_file() {
                    return Err(Error::IoError(std::io::Error::from(
                        std::io::ErrorKind::NotFound,
                    )));
                }

                let file = tokio::fs::File::open(&path).await?;

                let metadata = file.metadata().await?;

                let len = metadata.len() as _;

                if len == 0 || len > MAX_IMAGE_SIZE {
                    return Err(Error::InvalidLength {
                        context: "profile banner".into(),
                        current: len,
                        minimum: Some(1),
                        maximum: Some(MAX_IMAGE_SIZE),
                    });
                }

                let extension = path
                    .extension()
                    .and_then(OsStr::to_str)
                    .map(ExtensionType::from)
                    .unwrap_or(ExtensionType::Other);

                tracing::trace!("image size = {}", len);

                let stream =
                    ReaderStream::from_reader_with_cap(file.compat(), 512, Some(2 * 1024 * 1024));

                (OptType::Picture(Some(extension)), stream.boxed())
            }
            #[cfg(target_arch = "wasm32")]
            IdentityUpdate::BannerPath(_) => {
                return Err(Error::Unimplemented);
            }
            IdentityUpdate::BannerStream(stream) => {
                let stream = ReaderStream::from_reader_with_cap(
                    stream.into_async_read(),
                    512,
                    Some(MAX_IMAGE_SIZE),
                );
                (OptType::Banner(None), stream.boxed())
            }
            //TODO: Likely tie this to the store itself when it comes to pushing the update logic to `IdentityStore`
            IdentityUpdate::AddMetadataKey { key, value } => {
                let root = store.root_document();
                root.add_metadata_key(key, value).await?;

                let _ = store.export_root_document().await;
                store.push_to_all().await;
                return Ok(());
            }
            IdentityUpdate::RemoveMetadataKey { key } => {
                let root = store.root_document();
                root.remove_metadata_key(key).await?;

                let _ = store.export_root_document().await;
                store.push_to_all().await;
                return Ok(());
            }
        };

        let data = ByteCollection::new(stream).await?;

        let format = match opt {
            OptType::Picture(Some(format)) => format,
            OptType::Banner(Some(format)) => format,
            OptType::Picture(None) | OptType::Banner(None) => {
                tracing::trace!("image size = {}", data.len());

                let cursor = std::io::Cursor::new(&data);

                let image = image::ImageReader::new(cursor).with_guessed_format()?;

                image
                    .format()
                    .and_then(|format| ExtensionType::try_from(format).ok())
                    .unwrap_or(ExtensionType::Other)
            }
        };

        let cid = store::document::image_dag::store_photo(
            &ipfs,
            data,
            format.into(),
            Some(MAX_IMAGE_SIZE),
        )
        .await?;

        tracing::debug!("Image cid: {cid}");

        match opt {
            OptType::Picture(_) => {
                if let Some(picture_cid) = identity.metadata.profile_picture {
                    if picture_cid == cid {
                        tracing::debug!("Picture is already on document. Not updating identity");
                        return Ok(());
                    }

                    if let Some(banner_cid) = identity.metadata.profile_banner {
                        if picture_cid != banner_cid {
                            old_cid = Some(picture_cid);
                        }
                    }
                }

                identity.metadata.profile_picture = Some(cid);
            }
            OptType::Banner(_) => {
                if let Some(banner_cid) = identity.metadata.profile_banner {
                    if banner_cid == cid {
                        tracing::debug!("Banner is already on document. Not updating identity");
                        return Ok(());
                    }

                    if let Some(picture_cid) = identity.metadata.profile_picture {
                        if picture_cid != banner_cid {
                            old_cid = Some(banner_cid);
                        }
                    }
                }

                identity.metadata.profile_banner = Some(cid);
            }
        }

        if let Some(cid) = old_cid {
            if let Err(e) = store.delete_photo(cid).await {
                tracing::error!("Error deleting picture: {e}");
            }
        }

        store.identity_update(identity).await
    }

    fn tesseract(&self) -> Tesseract {
        self.tesseract.clone()
    }
}

#[async_trait::async_trait]
impl MultiPassImportExport for WarpIpfs {
    async fn import_identity<'a>(
        &mut self,
        option: IdentityImportOption<'a>,
    ) -> Result<Identity, Error> {
        if self.inner.components.read().is_some() {
            return Err(Error::IdentityExist);
        }
        let _g = self.inner.identity_guard.lock().await;
        if !self.tesseract.is_unlock() {
            return Err(Error::TesseractLocked);
        }
        match option {
            IdentityImportOption::Locate {
                location: ImportLocation::Local { path },
                passphrase,
            } => {
                let keypair = warp::crypto::keypair::did_from_mnemonic(&passphrase, None)?;

                let bytes = Zeroizing::new(keypair.private_key_bytes());
                let internal_keypair =
                    Keypair::ed25519_from_bytes(bytes).map_err(|_| Error::PrivateKeyInvalid)?;

                let bytes = fs::read(path).await?;

                let decrypted_bundle = ecdh_decrypt(&internal_keypair, None, bytes)?;
                let exported_document =
                    serde_json::from_slice::<ResolvedRootDocument>(&decrypted_bundle)?;

                exported_document.verify()?;

                warp::crypto::keypair::mnemonic_into_tesseract(
                    &self.tesseract,
                    &passphrase,
                    None,
                    self.inner.config.save_phrase(),
                    false,
                )?;

                self.init_ipfs(internal_keypair).await?;

                let mut store = self.identity_store(false).await?;

                store.import_identity(exported_document).await
            }
            IdentityImportOption::Locate {
                location: ImportLocation::Memory { buffer },
                passphrase,
            } => {
                let keypair = warp::crypto::keypair::did_from_mnemonic(&passphrase, None)?;

                let bytes = Zeroizing::new(keypair.private_key_bytes());
                let internal_keypair =
                    Keypair::ed25519_from_bytes(bytes).map_err(|_| Error::PrivateKeyInvalid)?;

                let bytes = std::mem::take(buffer);

                let decrypted_bundle = ecdh_decrypt(&internal_keypair, None, bytes)?;

                let exported_document =
                    serde_json::from_slice::<ResolvedRootDocument>(&decrypted_bundle)?;

                exported_document.verify()?;

                warp::crypto::keypair::mnemonic_into_tesseract(
                    &self.tesseract,
                    &passphrase,
                    None,
                    self.inner.config.save_phrase(),
                    false,
                )?;

                self.init_ipfs(internal_keypair).await?;

                let mut store = self.identity_store(false).await?;

                store.import_identity(exported_document).await
            }
            IdentityImportOption::Locate {
                location: ImportLocation::Remote,
                passphrase,
            } => {
                let keypair = warp::crypto::keypair::did_from_mnemonic(&passphrase, None)?;
                let bytes = Zeroizing::new(keypair.private_key_bytes());
                let internal_keypair =
                    Keypair::ed25519_from_bytes(bytes).map_err(|_| Error::PrivateKeyInvalid)?;

                warp::crypto::keypair::mnemonic_into_tesseract(
                    &self.tesseract,
                    &passphrase,
                    None,
                    self.inner.config.save_phrase(),
                    false,
                )?;

                self.init_ipfs(internal_keypair).await?;

                let mut store = self.identity_store(false).await?;

                store.import_identity_remote_resolve().await
            }
        }
    }

    async fn export_identity<'a>(&mut self, location: ImportLocation<'a>) -> Result<(), Error> {
        let store = self.identity_store(true).await?;

        match location {
            ImportLocation::Local { path } => {
                let bundle = store.root_document().export_bytes().await?;
                fs::write(path, bundle).await?;
                Ok(())
            }
            ImportLocation::Memory { buffer } => {
                *buffer = store.root_document().export_bytes().await?;
                Ok(())
            }
            ImportLocation::Remote => {
                store.export_root_document().await?;
                Ok(())
            }
        }
    }
}

#[async_trait::async_trait]
impl Friends for WarpIpfs {
    async fn send_request(&mut self, pubkey: &DID) -> Result<(), Error> {
        let mut store = self.identity_store(true).await?;
        store.send_request(pubkey).await
    }

    async fn accept_request(&mut self, pubkey: &DID) -> Result<(), Error> {
        let mut store = self.identity_store(true).await?;
        store.accept_request(pubkey).await
    }

    async fn deny_request(&mut self, pubkey: &DID) -> Result<(), Error> {
        let mut store = self.identity_store(true).await?;
        store.reject_request(pubkey).await
    }

    async fn close_request(&mut self, pubkey: &DID) -> Result<(), Error> {
        let mut store = self.identity_store(true).await?;
        store.close_request(pubkey).await
    }

    async fn list_incoming_request(&self) -> Result<Vec<FriendRequest>, Error> {
        let store = self.identity_store(true).await?;
        store.list_incoming_request().await
    }

    async fn list_outgoing_request(&self) -> Result<Vec<FriendRequest>, Error> {
        let store = self.identity_store(true).await?;
        store.list_outgoing_request().await
    }

    async fn received_friend_request_from(&self, did: &DID) -> Result<bool, Error> {
        let store = self.identity_store(true).await?;
        store.received_friend_request_from(did).await
    }

    async fn sent_friend_request_to(&self, did: &DID) -> Result<bool, Error> {
        let store = self.identity_store(true).await?;
        store.sent_friend_request_to(did).await
    }

    async fn remove_friend(&mut self, pubkey: &DID) -> Result<(), Error> {
        let mut store = self.identity_store(true).await?;
        store.remove_friend(pubkey, true).await
    }

    async fn block(&mut self, pubkey: &DID) -> Result<(), Error> {
        let mut store = self.identity_store(true).await?;
        store.block(pubkey).await
    }

    async fn is_blocked(&self, did: &DID) -> Result<bool, Error> {
        let store = self.identity_store(true).await?;
        store.is_blocked(did).await
    }

    async fn unblock(&mut self, pubkey: &DID) -> Result<(), Error> {
        let mut store = self.identity_store(true).await?;
        store.unblock(pubkey).await
    }

    async fn block_list(&self) -> Result<Vec<DID>, Error> {
        let store = self.identity_store(true).await?;
        store.block_list().await.map(Vec::from_iter)
    }

    async fn list_friends(&self) -> Result<Vec<DID>, Error> {
        let store = self.identity_store(true).await?;
        store.friends_list().await.map(Vec::from_iter)
    }

    async fn has_friend(&self, pubkey: &DID) -> Result<bool, Error> {
        let store = self.identity_store(true).await?;
        store.is_friend(pubkey).await
    }
}

#[async_trait::async_trait]
impl MultiPassEvent for WarpIpfs {
    async fn multipass_subscribe(&mut self) -> Result<MultiPassEventStream, Error> {
        let store = self.identity_store(true).await?;
        store.subscribe().await
    }
}

#[async_trait::async_trait]
impl IdentityInformation for WarpIpfs {
    async fn identity_picture(&self, did: &DID) -> Result<IdentityImage, Error> {
        let store = self.identity_store(true).await?;
        store.identity_picture(did).await
    }

    async fn identity_banner(&self, did: &DID) -> Result<IdentityImage, Error> {
        let store = self.identity_store(true).await?;
        store.identity_banner(did).await
    }

    async fn identity_status(&self, did: &DID) -> Result<identity::IdentityStatus, Error> {
        let store = self.identity_store(true).await?;
        store.identity_status(did).await
    }

    async fn set_identity_status(&mut self, status: identity::IdentityStatus) -> Result<(), Error> {
        let mut store = self.identity_store(true).await?;
        store.set_identity_status(status).await
    }

    async fn identity_platform(&self, did: &DID) -> Result<identity::Platform, Error> {
        let store = self.identity_store(true).await?;
        store.identity_platform(did).await
    }

    async fn identity_relationship(&self, did: &DID) -> Result<identity::Relationship, Error> {
        let store = self.identity_store(true).await?;
        store.lookup(did).await?;
        let friends = store.is_friend(did).await?;
        let received_friend_request = store.received_friend_request_from(did).await?;
        let sent_friend_request = store.sent_friend_request_to(did).await?;
        let blocked = store.is_blocked(did).await?;
        let blocked_by = store.is_blocked_by(did).await?;

        let mut relationship = Relationship::default();
        relationship.set_friends(friends);
        relationship.set_received_friend_request(received_friend_request);
        relationship.set_sent_friend_request(sent_friend_request);
        relationship.set_blocked(blocked);
        relationship.set_blocked_by(blocked_by);

        Ok(relationship)
    }
}

#[async_trait::async_trait]
impl RayGun for WarpIpfs {
    async fn create_conversation(&mut self, did_key: &DID) -> Result<Conversation, Error> {
        self.messaging_store()?.create_conversation(did_key).await
    }

    async fn create_group_conversation<P: Into<GroupPermissionOpt> + Send + Sync>(
        &mut self,
        name: Option<String>,
        recipients: Vec<DID>,
        permissions: P,
    ) -> Result<Conversation, Error> {
        self.messaging_store()?
            .create_group_conversation(name, HashSet::from_iter(recipients), permissions)
            .await
    }

    async fn get_conversation(&self, conversation_id: Uuid) -> Result<Conversation, Error> {
        self.messaging_store()?
            .get_conversation(conversation_id)
            .await
    }

    async fn set_favorite_conversation(
        &mut self,
        conversation_id: Uuid,
        favorite: bool,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .set_favorite_conversation(conversation_id, favorite)
            .await
    }

    async fn list_conversations(&self) -> Result<Vec<Conversation>, Error> {
        self.messaging_store()?.list_conversations().await
    }

    async fn get_message_count(&self, conversation_id: Uuid) -> Result<usize, Error> {
        self.messaging_store()?
            .messages_count(conversation_id)
            .await
    }

    async fn get_message(&self, conversation_id: Uuid, message_id: Uuid) -> Result<Message, Error> {
        self.messaging_store()?
            .get_message(conversation_id, message_id)
            .await
    }

    async fn get_message_references(
        &self,
        conversation_id: Uuid,
        opt: MessageOptions,
    ) -> Result<BoxStream<'static, MessageReference>, Error> {
        self.messaging_store()?
            .get_message_references(conversation_id, opt)
            .await
    }

    async fn get_message_reference(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageReference, Error> {
        self.messaging_store()?
            .get_message_reference(conversation_id, message_id)
            .await
    }

    async fn message_status(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageStatus, Error> {
        self.messaging_store()?
            .message_status(conversation_id, message_id)
            .await
    }

    async fn get_messages(
        &self,
        conversation_id: Uuid,
        opt: MessageOptions,
    ) -> Result<Messages, Error> {
        self.messaging_store()?
            .get_messages(conversation_id, opt)
            .await
    }

    async fn send(&mut self, conversation_id: Uuid, value: Vec<String>) -> Result<Uuid, Error> {
        self.messaging_store()?
            .send_message(conversation_id, value)
            .await
    }

    async fn edit(
        &mut self,
        conversation_id: Uuid,
        message_id: Uuid,
        value: Vec<String>,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .edit_message(conversation_id, message_id, value)
            .await
    }

    async fn delete(
        &mut self,
        conversation_id: Uuid,
        message_id: Option<Uuid>,
    ) -> Result<(), Error> {
        let store = self.messaging_store()?;
        match message_id {
            Some(id) => store.delete_message(conversation_id, id).await,
            None => store.delete_conversation(conversation_id).await.map(|_| ()),
        }
    }

    async fn react(
        &mut self,
        conversation_id: Uuid,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .react(conversation_id, message_id, state, emoji)
            .await
    }

    async fn pin(
        &mut self,
        conversation_id: Uuid,
        message_id: Uuid,
        state: PinState,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .pin_message(conversation_id, message_id, state)
            .await
    }

    async fn reply(
        &mut self,
        conversation_id: Uuid,
        message_id: Uuid,
        value: Vec<String>,
    ) -> Result<Uuid, Error> {
        self.messaging_store()?
            .reply(conversation_id, message_id, value)
            .await
    }

    async fn embeds(&mut self, _: Uuid, _: Uuid, _: EmbedState) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    async fn update_conversation_permissions<P: Into<GroupPermissionOpt> + Send + Sync>(
        &mut self,
        conversation_id: Uuid,
        permissions: P,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .update_conversation_permissions(conversation_id, permissions)
            .await
    }

    async fn update_conversation_icon(
        &mut self,
        conversation_id: Uuid,
        location: Location,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .update_conversation_icon(conversation_id, location)
            .await
    }

    async fn update_conversation_banner(
        &mut self,
        conversation_id: Uuid,
        location: Location,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .update_conversation_banner(conversation_id, location)
            .await
    }

    async fn conversation_icon(&self, conversation_id: Uuid) -> Result<ConversationImage, Error> {
        self.messaging_store()?
            .conversation_icon(conversation_id)
            .await
    }

    async fn conversation_banner(&self, conversation_id: Uuid) -> Result<ConversationImage, Error> {
        self.messaging_store()?
            .conversation_banner(conversation_id)
            .await
    }

    async fn remove_conversation_icon(&mut self, conversation_id: Uuid) -> Result<(), Error> {
        self.messaging_store()?
            .remove_conversation_icon(conversation_id)
            .await
    }

    async fn remove_conversation_banner(&mut self, conversation_id: Uuid) -> Result<(), Error> {
        self.messaging_store()?
            .remove_conversation_banner(conversation_id)
            .await
    }

    async fn archived_conversation(&mut self, conversation_id: Uuid) -> Result<(), Error> {
        self.messaging_store()?
            .archived_conversation(conversation_id)
            .await
    }

    async fn unarchived_conversation(&mut self, conversation_id: Uuid) -> Result<(), Error> {
        self.messaging_store()?
            .unarchived_conversation(conversation_id)
            .await
    }
}

#[async_trait::async_trait]
impl RayGunAttachment for WarpIpfs {
    async fn attach(
        &mut self,
        conversation_id: Uuid,
        message_id: Option<Uuid>,
        locations: Vec<Location>,
        message: Vec<String>,
    ) -> Result<(Uuid, AttachmentEventStream), Error> {
        self.messaging_store()?
            .attach(conversation_id, message_id, locations, message)
            .await
    }

    async fn download(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
        file: String,
        path: PathBuf,
    ) -> Result<ConstellationProgressStream, Error> {
        self.messaging_store()?
            .download(conversation_id, message_id, &file, path)
            .await
    }

    async fn download_stream(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
        file: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, std::io::Error>>, Error> {
        self.messaging_store()?
            .download_stream(conversation_id, message_id, file)
            .await
    }
}

#[async_trait::async_trait]
impl RayGunCommunity for WarpIpfs {
    async fn get_community_stream(
        &mut self,
        community_id: Uuid,
    ) -> Result<MessageEventStream, Error> {
        let store = self.messaging_store()?;
        let stream = store.get_community_stream(community_id).await?;
        Ok(stream.boxed())
    }

    async fn create_community(&mut self, name: &str) -> Result<Community, Error> {
        self.messaging_store()?.create_community(name).await
    }
    async fn delete_community(&mut self, community_id: Uuid) -> Result<(), Error> {
        self.messaging_store()?.delete_community(community_id).await
    }
    async fn get_community(&self, community_id: Uuid) -> Result<Community, Error> {
        self.messaging_store()?.get_community(community_id).await
    }

    async fn list_communities_joined(&self) -> Result<IndexSet<Uuid>, Error> {
        self.messaging_store()?.list_communities_joined().await
    }
    async fn list_communities_invited_to(&self) -> Result<Vec<(Uuid, CommunityInvite)>, Error> {
        self.messaging_store()?.list_communities_invited_to().await
    }
    async fn leave_community(&mut self, community_id: Uuid) -> Result<(), Error> {
        self.messaging_store()?.leave_community(community_id).await
    }

    async fn get_community_icon(&self, community_id: Uuid) -> Result<ConversationImage, Error> {
        self.messaging_store()?
            .get_community_icon(community_id)
            .await
    }
    async fn get_community_banner(&self, community_id: Uuid) -> Result<ConversationImage, Error> {
        self.messaging_store()?
            .get_community_banner(community_id)
            .await
    }
    async fn edit_community_icon(
        &mut self,
        community_id: Uuid,
        location: Location,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .edit_community_icon(community_id, location)
            .await
    }
    async fn edit_community_banner(
        &mut self,
        community_id: Uuid,
        location: Location,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .edit_community_banner(community_id, location)
            .await
    }

    async fn create_community_invite(
        &mut self,
        community_id: Uuid,
        target_user: Option<DID>,
        expiry: Option<DateTime<Utc>>,
    ) -> Result<CommunityInvite, Error> {
        self.messaging_store()?
            .create_community_invite(community_id, target_user, expiry)
            .await
    }
    async fn delete_community_invite(
        &mut self,
        community_id: Uuid,
        invite_id: Uuid,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .delete_community_invite(community_id, invite_id)
            .await
    }
    async fn get_community_invite(
        &self,
        community_id: Uuid,
        invite_id: Uuid,
    ) -> Result<CommunityInvite, Error> {
        self.messaging_store()?
            .get_community_invite(community_id, invite_id)
            .await
    }
    async fn request_join_community(&mut self, community_id: Uuid) -> Result<(), Error> {
        self.messaging_store()?
            .request_join_community(community_id)
            .await
    }
    async fn edit_community_invite(
        &mut self,
        community_id: Uuid,
        invite_id: Uuid,
        invite: CommunityInvite,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .edit_community_invite(community_id, invite_id, invite)
            .await
    }

    async fn create_community_role(
        &mut self,
        community_id: Uuid,
        name: &str,
    ) -> Result<CommunityRole, Error> {
        self.messaging_store()?
            .create_community_role(community_id, name)
            .await
    }
    async fn delete_community_role(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .delete_community_role(community_id, role_id)
            .await
    }
    async fn get_community_role(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
    ) -> Result<CommunityRole, Error> {
        self.messaging_store()?
            .get_community_role(community_id, role_id)
            .await
    }
    async fn edit_community_role_name(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
        new_name: String,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .edit_community_role_name(community_id, role_id, new_name)
            .await
    }
    async fn grant_community_role(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
        user: DID,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .grant_community_role(community_id, role_id, user)
            .await
    }
    async fn revoke_community_role(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
        user: DID,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .revoke_community_role(community_id, role_id, user)
            .await
    }

    async fn create_community_channel(
        &mut self,
        community_id: Uuid,
        channel_name: &str,
        channel_type: CommunityChannelType,
    ) -> Result<CommunityChannel, Error> {
        self.messaging_store()?
            .create_community_channel(community_id, channel_name, channel_type)
            .await
    }
    async fn delete_community_channel(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .delete_community_channel(community_id, channel_id)
            .await
    }
    async fn get_community_channel(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
    ) -> Result<CommunityChannel, Error> {
        self.messaging_store()?
            .get_community_channel(community_id, channel_id)
            .await
    }

    async fn edit_community_name(&mut self, community_id: Uuid, name: &str) -> Result<(), Error> {
        self.messaging_store()?
            .edit_community_name(community_id, name)
            .await
    }
    async fn edit_community_description(
        &mut self,
        community_id: Uuid,
        description: Option<String>,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .edit_community_description(community_id, description)
            .await
    }
    async fn grant_community_permission<T>(
        &mut self,
        community_id: Uuid,
        permission: T,
        role_id: RoleId,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .grant_community_permission(community_id, permission.to_string(), role_id)
            .await
    }
    async fn revoke_community_permission<T>(
        &mut self,
        community_id: Uuid,
        permission: T,
        role_id: RoleId,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .revoke_community_permission(community_id, permission.to_string(), role_id)
            .await
    }
    async fn grant_community_permission_for_all<T>(
        &mut self,
        community_id: Uuid,
        permission: T,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .grant_community_permission_for_all(community_id, permission.to_string())
            .await
    }
    async fn revoke_community_permission_for_all<T>(
        &mut self,
        community_id: Uuid,
        permission: T,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .revoke_community_permission_for_all(community_id, permission.to_string())
            .await
    }
    async fn has_community_permission<T>(
        &mut self,
        community_id: Uuid,
        permission: T,
        member: DID,
    ) -> Result<bool, Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .has_community_permission(community_id, permission.to_string(), member)
            .await
    }
    async fn remove_community_member(
        &mut self,
        community_id: Uuid,
        member: DID,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .remove_community_member(community_id, member)
            .await
    }

    async fn edit_community_channel_name(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        name: &str,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .edit_community_channel_name(community_id, channel_id, name)
            .await
    }
    async fn edit_community_channel_description(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        description: Option<String>,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .edit_community_channel_description(community_id, channel_id, description)
            .await
    }
    async fn grant_community_channel_permission<T>(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: T,
        role_id: RoleId,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .grant_community_channel_permission(
                community_id,
                channel_id,
                permission.to_string(),
                role_id,
            )
            .await
    }
    async fn revoke_community_channel_permission<T>(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: T,
        role_id: RoleId,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .revoke_community_channel_permission(
                community_id,
                channel_id,
                permission.to_string(),
                role_id,
            )
            .await
    }
    async fn grant_community_channel_permission_for_all<T>(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: T,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .grant_community_channel_permission_for_all(
                community_id,
                channel_id,
                permission.to_string(),
            )
            .await
    }
    async fn revoke_community_channel_permission_for_all<T>(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: T,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .revoke_community_channel_permission_for_all(
                community_id,
                channel_id,
                permission.to_string(),
            )
            .await
    }
    async fn has_community_channel_permission<T>(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: T,
        member: DID,
    ) -> Result<bool, Error>
    where
        T: ToString + Send,
    {
        self.messaging_store()?
            .has_community_channel_permission(
                community_id,
                channel_id,
                permission.to_string(),
                member,
            )
            .await
    }

    async fn get_community_channel_message(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<Message, Error> {
        self.messaging_store()?
            .get_community_channel_message(community_id, channel_id, message_id)
            .await
    }
    async fn get_community_channel_messages(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        options: MessageOptions,
    ) -> Result<Messages, Error> {
        self.messaging_store()?
            .get_community_channel_messages(community_id, channel_id, options)
            .await
    }
    async fn get_community_channel_message_count(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
    ) -> Result<usize, Error> {
        self.messaging_store()?
            .get_community_channel_message_count(community_id, channel_id)
            .await
    }
    async fn get_community_channel_message_reference(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageReference, Error> {
        self.messaging_store()?
            .get_community_channel_message_reference(community_id, channel_id, message_id)
            .await
    }
    async fn get_community_channel_message_references(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        options: MessageOptions,
    ) -> Result<BoxStream<'static, MessageReference>, Error> {
        self.messaging_store()?
            .get_community_channel_message_references(community_id, channel_id, options)
            .await
    }
    async fn community_channel_message_status(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageStatus, Error> {
        self.messaging_store()?
            .community_channel_message_status(community_id, channel_id, message_id)
            .await
    }
    async fn send_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message: Vec<String>,
    ) -> Result<Uuid, Error> {
        self.messaging_store()?
            .send_community_channel_message(community_id, channel_id, message)
            .await
    }
    async fn edit_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        message: Vec<String>,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .edit_community_channel_message(community_id, channel_id, message_id, message)
            .await
    }
    async fn reply_to_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        message: Vec<String>,
    ) -> Result<Uuid, Error> {
        self.messaging_store()?
            .reply_to_community_channel_message(community_id, channel_id, message_id, message)
            .await
    }
    async fn delete_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .delete_community_channel_message(community_id, channel_id, message_id)
            .await
    }
    async fn pin_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        state: PinState,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .pin_community_channel_message(community_id, channel_id, message_id, state)
            .await
    }
    async fn react_to_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .react_to_community_channel_message(community_id, channel_id, message_id, state, emoji)
            .await
    }
    async fn send_community_channel_messsage_event(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .send_community_channel_messsage_event(community_id, channel_id, event)
            .await
    }
    async fn cancel_community_channel_messsage_event(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .cancel_community_channel_messsage_event(community_id, channel_id, event)
            .await
    }
    async fn attach_to_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Option<Uuid>,
        locations: Vec<Location>,
        message: Vec<String>,
    ) -> Result<(Uuid, AttachmentEventStream), Error> {
        self.messaging_store()?
            .attach_to_community_channel_message(
                community_id,
                channel_id,
                message_id,
                locations,
                message,
            )
            .await
    }
    async fn download_from_community_channel_message(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        file: String,
        path: PathBuf,
    ) -> Result<ConstellationProgressStream, Error> {
        self.messaging_store()?
            .download_from_community_channel_message(
                community_id,
                channel_id,
                message_id,
                file,
                path,
            )
            .await
    }
    async fn download_stream_from_community_channel_message(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        file: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, std::io::Error>>, Error> {
        self.messaging_store()?
            .download_stream_from_community_channel_message(
                community_id,
                channel_id,
                message_id,
                file,
            )
            .await
    }
}

#[async_trait::async_trait]
impl RayGunGroupConversation for WarpIpfs {
    async fn update_conversation_name(
        &mut self,
        conversation_id: Uuid,
        name: &str,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .update_conversation_name(conversation_id, name)
            .await
    }

    async fn add_recipient(&mut self, conversation_id: Uuid, did_key: &DID) -> Result<(), Error> {
        self.messaging_store()?
            .add_participant(conversation_id, did_key)
            .await
    }

    async fn remove_recipient(
        &mut self,
        conversation_id: Uuid,
        did_key: &DID,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .remove_participant(conversation_id, did_key)
            .await
    }
}

#[async_trait::async_trait]
impl RayGunStream for WarpIpfs {
    async fn raygun_subscribe(&mut self) -> Result<RayGunEventStream, Error> {
        let rx = self.raygun_tx.subscribe().await?;
        Ok(rx)
    }

    async fn get_conversation_stream(
        &mut self,
        conversation_id: Uuid,
    ) -> Result<MessageEventStream, Error> {
        let store = self.messaging_store()?;
        let stream = store.get_conversation_stream(conversation_id).await?;
        Ok(stream.boxed())
    }
}

#[async_trait::async_trait]
impl RayGunEvents for WarpIpfs {
    async fn send_event(
        &mut self,
        conversation_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .send_event(conversation_id, event)
            .await
    }

    async fn cancel_event(
        &mut self,
        conversation_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .cancel_event(conversation_id, event)
            .await
    }
}

#[async_trait::async_trait]
impl RayGunConversationInformation for WarpIpfs {
    async fn set_conversation_description(
        &mut self,
        conversation_id: Uuid,
        description: Option<&str>,
    ) -> Result<(), Error> {
        self.messaging_store()?
            .set_description(conversation_id, description)
            .await
    }
}

#[async_trait::async_trait]
impl Constellation for WarpIpfs {
    fn modified(&self) -> DateTime<Utc> {
        self.file_store()
            .map(|store| store.modified())
            .unwrap_or(Utc::now())
    }

    fn root_directory(&self) -> Directory {
        self.file_store()
            .map(|store| store.root_directory())
            .unwrap_or_default()
    }

    fn max_size(&self) -> usize {
        self.file_store()
            .map(|store| store.max_size())
            .unwrap_or_default()
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put(&mut self, name: &str, path: &str) -> Result<ConstellationProgressStream, Error> {
        self.file_store()?.put(name, path).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn get(&self, name: &str, path: &str) -> Result<ConstellationProgressStream, Error> {
        self.file_store()?.get(name, path).await
    }

    async fn put_buffer(&mut self, name: &str, buffer: &[u8]) -> Result<(), Error> {
        self.file_store()?.put_buffer(name, buffer).await
    }

    async fn get_buffer(&self, name: &str) -> Result<Bytes, Error> {
        self.file_store()?.get_buffer(name).await
    }

    /// Used to upload file to the filesystem with data from a stream
    async fn put_stream(
        &mut self,
        name: &str,
        total_size: Option<usize>,
        stream: BoxStream<'static, std::io::Result<Bytes>>,
    ) -> Result<ConstellationProgressStream, Error> {
        self.file_store()?
            .put_stream(name, total_size, stream)
            .await
    }

    /// Used to download data from the filesystem using a stream
    async fn get_stream(
        &self,
        name: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, std::io::Error>>, Error> {
        self.file_store()?.get_stream(name).await
    }

    /// Used to remove data from the filesystem
    async fn remove(&mut self, name: &str, recursive: bool) -> Result<(), Error> {
        self.file_store()?.remove(name, recursive).await
    }

    async fn rename(&mut self, current: &str, new: &str) -> Result<(), Error> {
        self.file_store()?.rename(current, new).await
    }

    async fn create_directory(&mut self, name: &str, recursive: bool) -> Result<(), Error> {
        self.file_store()?.create_directory(name, recursive).await
    }

    async fn sync_ref(&mut self, path: &str) -> Result<(), Error> {
        self.file_store()?.sync_ref(path).await
    }

    fn set_path(&mut self, path: PathBuf) {
        if let Ok(mut store) = self.file_store() {
            store.set_path(path)
        }
    }

    fn get_path(&self) -> PathBuf {
        self.file_store()
            .map(|store| store.get_path())
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl ConstellationEvent for WarpIpfs {
    async fn constellation_subscribe(&mut self) -> Result<ConstellationEventStream, Error> {
        let rx = self.constellation_tx.subscribe().await?;
        Ok(ConstellationEventStream(rx))
    }
}

pub(crate) fn to_file_type(name: &str) -> FileType {
    let name = PathBuf::from(name.trim());
    let extension = name
        .extension()
        .and_then(OsStr::to_str)
        .map(ExtensionType::from)
        .unwrap_or(ExtensionType::Other);

    extension.into()
}

// pub mod ffi {
//     use crate::config::MpIpfsConfig;
//     use crate::IpfsIdentity;
//     use warp::async_on_block;
//     use warp::error::Error;
//     use warp::ffi::FFIResult;
//     use warp::multipass::MultiPassAdapter;
//     use warp::tesseract::Tesseract;

//     #[allow(clippy::missing_safety_doc)]
//     #[no_mangle]
//     pub unsafe extern "C" fn multipass_mp_ipfs_temporary(
//         tesseract: *const Tesseract,
//         config: *const MpIpfsConfig,
//     ) -> FFIResult<MultiPassAdapter> {
//         let tesseract = match tesseract.is_null() {
//             false => {
//                 let tesseract = &*tesseract;
//                 tesseract.clone()
//             }
//             true => Tesseract::default(),
//         };

//         let mut config = match config.is_null() {
//             true => MpIpfsConfig::testing(true),
//             false => (*config).clone(),
//         };

//         config.path = None;

//         let future = async move { IpfsIdentity::new(config, tesseract).await };

//         let account = match async_on_block(future) {
//             Ok(identity) => identity,
//             Err(e) => return FFIResult::err(Error::from(e)),
//         };

//         FFIResult::ok(MultiPassAdapter::new(Box::new(account)))
//     }

//     #[allow(clippy::missing_safety_doc)]
//     #[no_mangle]
//     pub unsafe extern "C" fn multipass_mp_ipfs_persistent(
//         tesseract: *const Tesseract,
//         config: *const MpIpfsConfig,
//     ) -> FFIResult<MultiPassAdapter> {
//         let tesseract = match tesseract.is_null() {
//             false => {
//                 let tesseract = &*tesseract;
//                 tesseract.clone()
//             }
//             true => Tesseract::default(),
//         };

//         let config = match config.is_null() {
//             true => {
//                 return FFIResult::err(Error::from(anyhow::anyhow!("Configuration is invalid")))
//             }
//             false => (*config).clone(),
//         };

//         let account = match async_on_block(IpfsIdentity::new(config, tesseract)) {
//             Ok(identity) => identity,
//             Err(e) => return FFIResult::err(Error::from(e)),
//         };

//         FFIResult::ok(MultiPassAdapter::new(Box::new(account)))
//     }
// }
