use ipld_core::cid::Cid;
use rust_ipfs::Keypair;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use warp::{crypto::DID, multipass::identity::ShortId};

use crate::store::identity::RequestResponsePayload;
use crate::store::{
    document::identity::IdentityDocument,
    payload::{PayloadBuilder, PayloadMessage},
};

pub fn payload_message_construct<T: Serialize + DeserializeOwned + Clone>(
    keypair: &Keypair,
    cosigner: Option<&Keypair>,
    message: T,
) -> Result<PayloadMessage<T>, anyhow::Error> {
    let mut payload = PayloadBuilder::new(keypair, message);
    if let Some(cosigner) = cosigner {
        payload = payload.cosign(cosigner);
    }
    let payload = payload.build()?;
    Ok(payload)
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Request {
    Register(Register),
    Mailbox(Mailbox),
    Synchronized(Synchronized),
    Lookup(Lookup),
}

impl From<Register> for Request {
    fn from(reg: Register) -> Self {
        Request::Register(reg)
    }
}

impl From<Mailbox> for Request {
    fn from(mailbox: Mailbox) -> Self {
        Request::Mailbox(mailbox)
    }
}

impl From<Synchronized> for Request {
    fn from(sync: Synchronized) -> Self {
        Request::Synchronized(sync)
    }
}

impl From<Lookup> for Request {
    fn from(lookup: Lookup) -> Self {
        Request::Lookup(lookup)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    RegisterResponse(RegisterResponse),
    SynchronizedResponse(SynchronizedResponse),
    MailboxResponse(MailboxResponse),
    LookupResponse(LookupResponse),
    Ack,
    InvalidPayload,
    Error(String),
}

impl From<RegisterResponse> for Response {
    fn from(res: RegisterResponse) -> Self {
        Response::RegisterResponse(res)
    }
}

impl From<SynchronizedResponse> for Response {
    fn from(res: SynchronizedResponse) -> Self {
        Response::SynchronizedResponse(res)
    }
}

impl From<MailboxResponse> for Response {
    fn from(res: MailboxResponse) -> Self {
        Response::MailboxResponse(res)
    }
}

impl From<LookupResponse> for Response {
    fn from(res: LookupResponse) -> Self {
        Response::LookupResponse(res)
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mailbox {
    FetchAll,
    FetchFrom {
        did: DID,
    },
    Send {
        did: DID,
        request: RequestResponsePayload,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MailboxResponse {
    Receive {
        list: Vec<RequestResponsePayload>,
        remaining: usize,
    },
    Removed,
    Completed,
    Sent,
    Error(MailboxError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MailboxError {
    IdentityNotRegistered,
    UserNotRegistered,
    NoRequests,
    Blocked,
    InvalidRequest,
    Other(String),
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lookup {
    // Locate { peer_id: PeerId, kind: LocateKind },
    Username { username: String, count: u8 },
    ShortId { short_id: ShortId },
    PublicKey { did: DID },
    PublicKeys { dids: Vec<DID> },
}

// #[derive(Clone, Debug, Serialize, Deserialize)]
// #[serde(rename_all = "snake_case")]
// pub enum LocateKind {
//     Record,
//     Connect,
// }

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LookupError {
    DoesntExist,
    RateExceeded,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LookupResponse {
    Ok { identity: Vec<IdentityDocument> },
    Error(LookupError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Register {
    IsRegistered,
    RegisterIdentity { root_cid: Cid },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegisterResponse {
    Ok,
    Error(RegisterError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegisterError {
    InternalError,
    IdentityExist,
    IdentityVerificationFailed,
    NotRegistered,
    None,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Synchronized {
    Store { package: Cid },
    Fetch,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SynchronizedResponse {
    RecordStored,
    IdentityUpdated,
    Package(Cid),
    Error(SynchronizedError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SynchronizedError {
    DoesntExist,
    Forbidden,
    NotRegistered,
    Invalid,
    InvalidPayload { msg: String },
    InvalodRecord { msg: String },
}
