pub mod community;
pub mod conversation;
pub mod discovery;
pub mod document;
pub mod event_subscription;
pub mod files;
pub mod identity;
pub mod keystore;
pub mod message;
pub mod payload;
pub mod phonebook;
pub mod queue;

use chrono::{DateTime, Utc};
use community::{CommunityChannelDocument, CommunityDocument, CommunityRoleDocument};
use rust_ipfs as ipfs;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::store::community::CommunityInviteDocument;
use ipfs::{libp2p::identity::KeyType, Keypair, PeerId, PublicKey};
use warp::{
    crypto::{
        cipher::Cipher,
        did_key::{Generate, ECDH},
        hash::sha256_hash,
        zeroize::Zeroizing,
        Ed25519KeyPair, KeyMaterial, DID,
    },
    error::Error,
    multipass::identity::IdentityStatus,
    raygun::{community::RoleId, GroupPermissions, MessageEvent, PinState, ReactionState},
};

use conversation::{message::MessageDocument, ConversationDocument};

pub const MAX_THUMBNAIL_SIZE: usize = 5_242_880;
pub const MAX_IMAGE_SIZE: usize = 2_097_152;
pub const MIN_USERNAME_LENGTH: usize = 4;
pub const MAX_USERNAME_LENGTH: usize = 64;
pub const MAX_STATUS_LENGTH: usize = 512;
pub const MIN_MESSAGE_SIZE: usize = 1;
pub const MAX_MESSAGE_SIZE: usize = 4_096;
pub const MAX_ATTACHMENT: usize = 32;
pub const MIN_ATTACHMENT: usize = 1;
pub const MAX_CONVERSATIONS: usize = 1_000;
pub const MAX_FRIENDS: usize = 1_000;
pub const MAX_REQUEST: usize = 1_000;
pub const MAX_METADATA_KEY_LENGTH: usize = 32;
pub const MAX_METADATA_VALUE_LENGTH: usize = 128;
pub const MAX_METADATA_ENTRIES: usize = 20;
pub const MAX_THUMBNAIL_STREAM_SIZE: usize = 20 * 1024 * 1024;
pub const MAX_CONVERSATION_ICON_SIZE: usize = 4 * 1024 * 1024;
pub const MAX_CONVERSATION_BANNER_SIZE: usize = 8 * 1024 * 1024;
pub const MAX_COMMUNITY_CHANNELS: usize = 20;

pub(crate) mod protocols {
    use rust_ipfs::libp2p::StreamProtocol;

    /// Protocol for exchanging direct metadata information (eg ping, document reference, etc)
    pub const EXCHANGE_PROTOCOL: StreamProtocol = StreamProtocol::new("/warp/exchange");
    /// Protocol for providing identity information and direct friend requests acknowledgement
    pub const IDENTITY_PROTOCOL: StreamProtocol = StreamProtocol::new("/warp/identity");
    /// Bootstrap discovery
    pub const DISCOVERY_PROTOCOL: StreamProtocol = StreamProtocol::new("/warp/discovery");

    // shuttle various protocols
    pub const SHUTTLE_IDENTITY: StreamProtocol = StreamProtocol::new("/shuttle/identity/0.0.1");
    pub const SHUTTLE_MESSAGE: StreamProtocol = StreamProtocol::new("/shuttle/message/0.0.1");
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConversationImageType {
    Icon,
    Banner,
}
pub const MAX_CONVERSATION_DESCRIPTION: usize = 256;
pub const MAX_COMMUNITY_DESCRIPTION: usize = 256;
pub const MAX_REACTIONS: usize = 30;

pub(super) mod topics {
    use std::fmt::Display;

    use uuid::Uuid;
    use warp::crypto::DID;

    /// Topic to announce identity updates to the network
    pub const IDENTITY_ANNOUNCEMENT: &str = "/identity/announce/v0";

    pub trait PeerTopic: Display {
        fn inbox(&self) -> String {
            format!("/id/{self}/inbox")
        }

        fn events(&self) -> String {
            format!("/id/{self}/events")
        }
        fn messaging(&self) -> String {
            format!("/id/{self}/messaging")
        }

        // fn invites(&self) -> String {
        //     format!("/id/{self}/messaging/invites")
        // }
    }

    impl PeerTopic for DID {}

    pub trait ConversationTopic: Display {
        fn base(&self) -> String;
        fn event_topic(&self) -> String {
            format!("{}/events", self.base())
        }

        fn exchange_topic(&self, did: &DID) -> String {
            format!("{}/exchange/{}", self.base(), did)
        }
        fn join_topic(&self) -> String {
            format!("{}/join", self.base())
        }
    }

    impl ConversationTopic for Uuid {
        fn base(&self) -> String {
            format!("/conversation/{self}")
        }
    }
}

pub(super) mod ds_key {
    use rust_ipfs::{Ipfs, Keypair, PeerId, PublicKey};

    pub trait DataStoreKey {
        fn base(&self) -> String;

        fn root(&self) -> String {
            self.base() + "/root"
        }

        fn cache(&self) -> String {
            self.base() + "/cache"
        }

        fn messaging_queue(&self) -> String {
            self.base() + "/messaging_queue"
        }

        fn request_queue(&self) -> String {
            self.base() + "/request_queue"
        }
    }

    impl DataStoreKey for Ipfs {
        fn base(&self) -> String {
            let peer_id = self.keypair().public().to_peer_id();
            format!("/identity/{peer_id}")
        }
    }

    impl DataStoreKey for PeerId {
        fn base(&self) -> String {
            format!("/identity/{self}")
        }
    }

    impl DataStoreKey for Keypair {
        fn base(&self) -> String {
            let peer_id = self.public().to_peer_id();
            format!("/identity/{peer_id}")
        }
    }

    impl DataStoreKey for PublicKey {
        fn base(&self) -> String {
            let peer_id = self.to_peer_id();
            format!("/identity/{peer_id}")
        }
    }
}

pub trait PeerIdExt {
    fn to_public_key(&self) -> Result<PublicKey, anyhow::Error>;
    fn to_did(&self) -> Result<DID, anyhow::Error>;
}

impl PeerIdExt for PeerId {
    fn to_public_key(&self) -> Result<PublicKey, anyhow::Error> {
        let multihash = self.as_ref();
        if multihash.code() != 0 {
            anyhow::bail!("PeerId does not contain inline public key");
        }
        let public_key = PublicKey::try_decode_protobuf(multihash.digest())?;
        Ok(public_key)
    }

    fn to_did(&self) -> Result<DID, anyhow::Error> {
        let multihash = self.as_ref();
        if multihash.code() != 0 {
            anyhow::bail!("PeerId does not contain inline public key");
        }
        let public_key = PublicKey::try_decode_protobuf(multihash.digest())?;
        sealed::libp2p_pub_to_did(&public_key)
    }
}

impl PeerIdExt for PublicKey {
    fn to_public_key(&self) -> Result<PublicKey, anyhow::Error> {
        Ok(self.clone())
    }

    fn to_did(&self) -> Result<DID, anyhow::Error> {
        sealed::libp2p_pub_to_did(self)
    }
}

impl PeerIdExt for Keypair {
    fn to_public_key(&self) -> Result<PublicKey, anyhow::Error> {
        Ok(self.public())
    }

    fn to_did(&self) -> Result<DID, anyhow::Error> {
        sealed::libp2p_pub_to_did(&self.public())
    }
}

pub trait DidExt {
    fn to_public_key(&self) -> Result<PublicKey, anyhow::Error>;
    fn to_peer_id(&self) -> Result<PeerId, anyhow::Error>;
    fn to_keypair(&self) -> Result<Keypair, anyhow::Error>;
}

impl DidExt for DID {
    fn to_public_key(&self) -> Result<PublicKey, anyhow::Error> {
        sealed::did_to_libp2p_pub(self)
    }

    fn to_peer_id(&self) -> Result<PeerId, anyhow::Error> {
        self.to_public_key().map(|p| p.to_peer_id())
    }

    fn to_keypair(&self) -> Result<Keypair, anyhow::Error> {
        use warp::crypto::ed25519_dalek::{
            PublicKey, SecretKey, KEYPAIR_LENGTH, SECRET_KEY_LENGTH,
        };

        let bytes = Zeroizing::new(self.private_key_bytes());
        let secret_key = SecretKey::from_bytes(&bytes)?;
        let public_key: PublicKey = (&secret_key).into();
        let mut bytes: Zeroizing<[u8; KEYPAIR_LENGTH]> = Zeroizing::new([0u8; KEYPAIR_LENGTH]);

        bytes[..SECRET_KEY_LENGTH].copy_from_slice(secret_key.as_bytes());
        bytes[SECRET_KEY_LENGTH..].copy_from_slice(public_key.as_bytes());

        let libp2p_keypair = Keypair::ed25519_from_bytes(bytes)?;

        Ok(libp2p_keypair)
    }
}

impl DidExt for &DID {
    fn to_public_key(&self) -> Result<PublicKey, anyhow::Error> {
        (*self).to_public_key()
    }

    fn to_peer_id(&self) -> Result<PeerId, anyhow::Error> {
        (*self).to_peer_id()
    }

    fn to_keypair(&self) -> Result<Keypair, anyhow::Error> {
        (*self).to_keypair()
    }
}

pub trait VecExt<T: Eq> {
    fn insert_item(&mut self, item: T) -> bool;
    fn remove_item(&mut self, item: &T) -> bool;
}

impl<T> VecExt<T> for Vec<T>
where
    T: Eq + Clone,
{
    fn insert_item(&mut self, item: T) -> bool {
        if self.contains(&item) {
            return false;
        }

        self.push(item);
        true
    }

    fn remove_item(&mut self, item: &T) -> bool {
        if !self.contains(item) {
            return false;
        }
        if let Some(index) = self.iter().position(|el| item.eq(el)) {
            self.remove(index);
            return true;
        }
        false
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ConversationEvents {
    NewConversation {
        recipient: DID,
    },
    NewGroupConversation {
        conversation: ConversationDocument,
    },
    LeaveConversation {
        conversation_id: Uuid,
        recipient: DID,
        signature: String,
    },
    DeleteConversation {
        conversation_id: Uuid,
    },

    NewCommunityInvite {
        community_id: Uuid,
        invite: CommunityInviteDocument,
    },
    DeleteCommunityInvite {
        community_id: Uuid,
        invite: CommunityInviteDocument,
    },
    JoinCommunity {
        community_id: Uuid,
        community_document: Option<CommunityDocument>,
    },
    DeleteCommunity {
        community_id: Uuid,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ConversationRequestResponse {
    Request {
        conversation_id: Uuid,
        kind: ConversationRequestKind,
    },
    Response {
        conversation_id: Uuid,
        kind: ConversationResponseKind,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConversationRequestKind {
    Acknowledge,
    Key,
    Ping,
    RetrieveMessages {
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    },
    WantMessage {
        message_id: Uuid,
    },
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConversationResponseKind {
    Key { key: Vec<u8> },
    Pong,
    HaveMessages { messages: Vec<Uuid> },
    AcknowledgementConfirmed,
}

impl std::fmt::Debug for ConversationResponseKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConversationRespondKind")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum MessagingEvents {
    New {
        message: MessageDocument,
    },
    Edit {
        conversation_id: Uuid,
        message_id: Uuid,
        modified: DateTime<Utc>,
        lines: Vec<String>,
        nonce: Vec<u8>,
        signature: Vec<u8>,
    },
    Delete {
        conversation_id: Uuid,
        message_id: Uuid,
    },
    Pin {
        conversation_id: Uuid,
        member: DID,
        message_id: Uuid,
        state: PinState,
    },
    React {
        conversation_id: Uuid,
        reactor: DID,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
    },
    UpdateConversation {
        conversation: ConversationDocument,
        kind: ConversationUpdateKind,
    },
    Event {
        conversation_id: Uuid,
        member: DID,
        event: MessageEvent,
        cancelled: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum CommunityMessagingEvents {
    New {
        community_id: Uuid,
        channel_id: Uuid,
        message: MessageDocument,
    },
    Edit {
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        modified: DateTime<Utc>,
        lines: Vec<String>,
        nonce: Vec<u8>,
        signature: Vec<u8>,
    },
    Delete {
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
    },
    Pin {
        community_id: Uuid,
        channel_id: Uuid,
        member: DID,
        message_id: Uuid,
        state: PinState,
    },
    React {
        community_id: Uuid,
        channel_id: Uuid,
        reactor: DID,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
    },
    JoinedCommunity {
        community_id: Uuid,
        user: DID,
    },
    UpdateCommunity {
        community: CommunityDocument,
        kind: CommunityUpdateKind,
    },
    Event {
        community_id: Uuid,
        channel_id: Uuid,
        member: DID,
        event: MessageEvent,
        cancelled: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum CommunityJoinEvents {
    Join,
    DeleteInvite { invite_id: Uuid },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ConversationUpdateKind {
    AddParticipant { did: DID },
    RemoveParticipant { did: DID },
    AddRestricted { did: DID },
    RemoveRestricted { did: DID },
    ChangeName { name: Option<String> },
    ChangePermissions { permissions: GroupPermissions },
    AddedIcon,
    AddedBanner,
    RemovedIcon,
    RemovedBanner,
    ChangeDescription { description: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum CommunityUpdateKind {
    LeaveCommunity,
    CreateCommunityInvite {
        invite: CommunityInviteDocument,
    },
    DeleteCommunityInvite {
        invite_id: Uuid,
    },
    EditCommunityInvite {
        invite_id: Uuid,
    },
    CreateCommunityRole {
        role: CommunityRoleDocument,
    },
    DeleteCommunityRole {
        role_id: RoleId,
    },
    EditCommunityRole {
        role_id: RoleId,
    },
    GrantCommunityRole {
        role_id: RoleId,
        user: DID,
    },
    RevokeCommunityRole {
        role_id: RoleId,
        user: DID,
    },
    CreateCommunityChannel {
        channel: CommunityChannelDocument,
    },
    DeleteCommunityChannel {
        channel_id: Uuid,
    },
    EditCommunityName {
        name: String,
    },
    EditCommunityDescription {
        description: Option<String>,
    },
    EditIcon,
    EditBanner,
    GrantCommunityPermission {
        permissions: Vec<String>,
        role_id: RoleId,
    },
    RevokeCommunityPermission {
        permissions: Vec<String>,
        role_id: RoleId,
    },
    GrantCommunityPermissionForAll {
        permissions: Vec<String>,
    },
    RevokeCommunityPermissionForAll {
        permissions: Vec<String>,
    },
    RemoveCommunityMember {
        member: DID,
    },
    EditCommunityChannelName {
        channel_id: Uuid,
        name: String,
    },
    EditCommunityChannelDescription {
        channel_id: Uuid,
        description: Option<String>,
    },
    GrantCommunityChannelPermission {
        channel_id: Uuid,
        permissions: Vec<String>,
        role_id: RoleId,
    },
    RevokeCommunityChannelPermission {
        channel_id: Uuid,
        permissions: Vec<String>,
        role_id: RoleId,
    },
    GrantCommunityChannelPermissionForAll {
        channel_id: Uuid,
        permissions: Vec<String>,
    },
    RevokeCommunityChannelPermissionForAll {
        channel_id: Uuid,
        permissions: Vec<String>,
    },
}

// Note that this are temporary
fn sign_serde<D: Serialize>(keypair: &Keypair, data: &D) -> anyhow::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(data)?;
    Ok(keypair.sign(&bytes).expect("not RSA"))
}

// Note that this are temporary
fn verify_serde_sig<D: Serialize>(pk: DID, data: &D, signature: &[u8]) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(data)?;
    let pk = pk.to_public_key()?;
    if !pk.verify(&bytes, signature) {
        return Err(Error::InvalidSignature.into());
    }

    Ok(())
}

pub fn generate_shared_topic(
    keypair: &Keypair,
    did_b: &DID,
    seed: Option<&str>,
) -> anyhow::Result<Uuid> {
    let did_a = sealed::get_keypair_did(keypair)?;
    let x25519_a = Ed25519KeyPair::from_secret_key(&did_a.private_key_bytes()).get_x25519();
    let x25519_b = Ed25519KeyPair::from_public_key(&did_b.public_key_bytes()).get_x25519();
    let shared_key = x25519_a.key_exchange(&x25519_b);
    let topic_hash = sha256_hash(&shared_key, seed.map(|s| s.as_bytes()));
    Uuid::from_slice(&topic_hash[..topic_hash.len() / 2]).map_err(anyhow::Error::from)
}

mod sealed {
    use rust_ipfs::{Keypair, PublicKey};
    use warp::crypto::KeyMaterial;
    use warp::crypto::{zeroize::Zeroizing, DIDKey, Ed25519KeyPair, DID};
    use warp::{crypto::did_key::Generate, error::Error};
    pub fn get_keypair_did(keypair: &Keypair) -> anyhow::Result<DID> {
        let kp = Zeroizing::new(keypair.clone().try_into_ed25519()?.to_bytes());
        let kp = warp::crypto::ed25519_dalek::Keypair::from_bytes(&*kp)?;
        let did = DIDKey::Ed25519(Ed25519KeyPair::from_secret_key(kp.secret.as_bytes()));
        Ok(did.into())
    }

    pub fn did_to_libp2p_pub(public_key: &DID) -> anyhow::Result<PublicKey> {
        let pub_key = rust_ipfs::libp2p::identity::ed25519::PublicKey::try_from_bytes(
            &public_key.public_key_bytes(),
        )?;
        Ok(PublicKey::from(pub_key))
    }

    pub fn libp2p_pub_to_did(public_key: &PublicKey) -> anyhow::Result<DID> {
        let pk = match public_key.clone().try_into_ed25519() {
            Ok(pk) => {
                let did: DIDKey = Ed25519KeyPair::from_public_key(&pk.to_bytes()).into();
                did.into()
            }
            _ => anyhow::bail!(Error::PublicKeyInvalid),
        };
        Ok(pk)
    }
}

pub(crate) fn ecdh_shared_key(
    keypair: &Keypair,
    recipient: Option<&DID>,
) -> Result<Vec<u8>, Error> {
    assert!(keypair.key_type() != KeyType::RSA);

    let did = sealed::get_keypair_did(keypair)?;

    let prikey = Ed25519KeyPair::from_secret_key(&did.private_key_bytes()).get_x25519();
    let did_pubkey = match recipient {
        Some(did) => did.public_key_bytes(),
        None => did.public_key_bytes(),
    };

    let pubkey = Ed25519KeyPair::from_public_key(&did_pubkey).get_x25519();
    let prik = prikey.key_exchange(&pubkey);

    Ok(prik)
}

pub(crate) fn ecdh_encrypt<K: AsRef<[u8]>>(
    did: &Keypair,
    recipient: Option<&DID>,
    data: K,
) -> Result<Vec<u8>, Error> {
    let prik = Zeroizing::new(ecdh_shared_key(did, recipient)?);
    let data = Cipher::direct_encrypt(data.as_ref(), &prik)?;

    Ok(data)
}

pub(crate) fn ecdh_encrypt_with_nonce<K: AsRef<[u8]>>(
    keypair: &Keypair,
    recipient: Option<&DID>,
    data: K,
    nonce: &[u8],
) -> Result<Vec<u8>, Error> {
    assert!(keypair.key_type() != KeyType::RSA);

    let did = sealed::get_keypair_did(keypair)?;
    let prikey = Ed25519KeyPair::from_secret_key(&did.private_key_bytes()).get_x25519();
    let did_pubkey = match recipient {
        Some(did) => did.public_key_bytes(),
        None => did.public_key_bytes(),
    };

    let pubkey = Ed25519KeyPair::from_public_key(&did_pubkey).get_x25519();
    let prik = Zeroizing::new(prikey.key_exchange(&pubkey));
    let data = Cipher::direct_encrypt_with_nonce(data.as_ref(), &prik, nonce)?;

    Ok(data)
}

pub(crate) fn ecdh_decrypt<K: AsRef<[u8]>>(
    keypair: &Keypair,
    recipient: Option<&DID>,
    data: K,
) -> Result<Vec<u8>, Error> {
    let prik = Zeroizing::new(ecdh_shared_key(keypair, recipient)?);
    let data = Cipher::direct_decrypt(data.as_ref(), &prik)?;

    Ok(data)
}

#[allow(clippy::large_enum_variant)]
pub enum PeerType {
    PeerId(PeerId),
    DID(DID),
}

impl From<&DID> for PeerType {
    fn from(did: &DID) -> Self {
        PeerType::DID(did.clone())
    }
}

impl From<DID> for PeerType {
    fn from(did: DID) -> Self {
        PeerType::DID(did)
    }
}

impl From<PeerId> for PeerType {
    fn from(peer_id: PeerId) -> Self {
        PeerType::PeerId(peer_id)
    }
}

impl From<&PeerId> for PeerType {
    fn from(peer_id: &PeerId) -> Self {
        PeerType::PeerId(*peer_id)
    }
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum PeerConnectionType {
    Connected,
    #[default]
    NotConnected,
}

impl From<PeerConnectionType> for IdentityStatus {
    fn from(status: PeerConnectionType) -> Self {
        match status {
            PeerConnectionType::Connected => IdentityStatus::Online,
            PeerConnectionType::NotConnected => IdentityStatus::Offline,
        }
    }
}

pub async fn connected_to_peer<I: Into<PeerType>>(
    ipfs: &ipfs::Ipfs,
    pkey: I,
) -> anyhow::Result<PeerConnectionType> {
    let peer_id = match pkey.into() {
        PeerType::DID(did) => did.to_peer_id()?,
        PeerType::PeerId(peer) => peer,
    };

    let connected_peer = ipfs.is_connected(peer_id).await?;

    Ok(match connected_peer {
        true => PeerConnectionType::Connected,
        false => PeerConnectionType::NotConnected,
    })
}

pub fn extract_data_slice<const N: usize>(data: &[u8]) -> (&[u8], &[u8]) {
    let extracted = &data[data.len() - N..];
    let payload = &data[..data.len() - N];
    (extracted, payload)
}

#[cfg(test)]
mod test {
    use rust_ipfs::Keypair;
    use warp::crypto::DID;

    use crate::store::DidExt;

    use super::PeerIdExt;

    #[test]
    fn peer_id_to_did() -> anyhow::Result<()> {
        let peer_id = generate_ed25519_keypair(0).public().to_peer_id();
        assert!(peer_id.to_did().is_ok());

        let random_did = DID::default();
        let public_key = random_did.to_public_key()?;

        let peer_id = public_key.to_peer_id();

        let same_did = peer_id.to_did()?;
        assert_eq!(same_did, random_did);

        Ok(())
    }

    fn generate_ed25519_keypair(seed: u8) -> Keypair {
        let mut buffer = [0u8; 32];
        buffer[0] = seed;
        Keypair::ed25519_from_bytes(buffer).expect("valid keypair")
    }
}
