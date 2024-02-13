use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use chrono::Utc;
use futures::{
    channel::{mpsc::Receiver, oneshot},
    SinkExt, StreamExt,
};
use libipld::Cid;
use rust_ipfs::{Ipfs, IpfsPath};
use tokio::select;
use tokio_util::sync::{CancellationToken, DropGuard};
use uuid::Uuid;
use warp::{crypto::DID, error::Error, multipass::identity::IdentityStatus};

use crate::store::{ecdh_encrypt, identity::Request, keystore::Keystore, VecExt};

use super::{identity::IdentityDocument, ExtractedRootDocument, RootDocument};

#[allow(clippy::large_enum_variant)]
pub enum RootDocumentCommand {
    Get {
        response: oneshot::Sender<Result<RootDocument, Error>>,
    },
    Set {
        document: RootDocument,
        response: oneshot::Sender<Result<(), Error>>,
    },
    Identity {
        response: oneshot::Sender<Result<IdentityDocument, Error>>,
    },
    SetIdentityStatus {
        status: IdentityStatus,
        response: oneshot::Sender<Result<(), Error>>,
    },
    AddFriend {
        did: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RemoveFriend {
        did: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetFriendList {
        response: oneshot::Sender<Result<Vec<DID>, Error>>,
    },
    AddRequest {
        request: Request,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RemoveRequest {
        request: Request,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetRequestList {
        response: oneshot::Sender<Result<Vec<Request>, Error>>,
    },
    AddBlock {
        did: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RemoveBlock {
        did: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetBlockList {
        response: oneshot::Sender<Result<Vec<DID>, Error>>,
    },
    AddBlockBy {
        did: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RemoveBlockBy {
        did: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetBlockByList {
        response: oneshot::Sender<Result<Vec<DID>, Error>>,
    },
    SetKeystore {
        document: BTreeMap<String, Cid>,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetKeystoreMap {
        response: oneshot::Sender<Result<BTreeMap<String, Cid>, Error>>,
    },
    GetKeystore {
        id: Uuid,
        response: oneshot::Sender<Result<Keystore, Error>>,
    },
    Export {
        response: oneshot::Sender<Result<ExtractedRootDocument, Error>>,
    },
    ExportEncrypted {
        response: oneshot::Sender<Result<Vec<u8>, Error>>,
    },
    RootCid {
        response: oneshot::Sender<Result<Cid, Error>>,
    },
    SetRootCid {
        cid: Cid,
        response: oneshot::Sender<Result<(), Error>>,
    },
}

#[derive(Debug, Clone)]
pub struct RootDocumentMap {
    tx: futures::channel::mpsc::Sender<RootDocumentCommand>,
    _task_cancellation: Arc<DropGuard>,
}

impl RootDocumentMap {
    pub async fn new(ipfs: &Ipfs, keypair: Arc<DID>, path: Option<PathBuf>) -> Self {
        let cid = match path.as_ref() {
            Some(path) => tokio::fs::read(path.join(".id"))
                .await
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                .ok()
                .and_then(|cid_str| cid_str.parse().ok()),
            None => None,
        };

        let (tx, rx) = futures::channel::mpsc::channel(0);

        let mut task = RootDocumentTask {
            ipfs: ipfs.clone(),
            keypair,
            path,
            cid,
            rx,
        };

        let token = CancellationToken::new();
        let drop_guard = token.clone().drop_guard();
        tokio::spawn(async move {
            select! {
                _ = token.cancelled() => {}
                _ = task.run() => {}
            }
        });

        Self {
            tx,
            _task_cancellation: Arc::new(drop_guard),
        }
    }

    pub async fn get(&self) -> Result<RootDocument, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::Get { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn set(&mut self, document: RootDocument) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::Set {
                document,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn identity(&self) -> Result<IdentityDocument, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::Identity { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn set_status_indicator(&self, status: IdentityStatus) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::SetIdentityStatus {
                status,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn add_friend(&self, did: &DID) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::AddFriend {
                did: did.clone(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn remove_friend(&self, did: &DID) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::RemoveFriend {
                did: did.clone(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn add_block(&self, did: &DID) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::AddBlock {
                did: did.clone(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn remove_block(&self, did: &DID) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::RemoveBlock {
                did: did.clone(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn add_block_by(&self, did: &DID) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::AddBlockBy {
                did: did.clone(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn remove_block_by(&self, did: &DID) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::RemoveBlockBy {
                did: did.clone(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn add_request(&self, request: &Request) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::AddRequest {
                request: request.clone(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn remove_request(&self, request: &Request) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::RemoveRequest {
                request: request.clone(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_friends(&self) -> Result<Vec<DID>, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::GetFriendList { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_requests(&self) -> Result<Vec<Request>, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::GetRequestList { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_blocks(&self) -> Result<Vec<DID>, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::GetBlockList { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_block_by(&self) -> Result<Vec<DID>, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::GetBlockByList { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn export(&self) -> Result<ExtractedRootDocument, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::Export { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn export_bytes(&self) -> Result<Vec<u8>, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::ExportEncrypted { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn root_cid(&self) -> Result<Cid, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::RootCid { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn set_root_cid(&self, cid: Cid) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::SetRootCid { cid, response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_conversation_keystore_map(&self) -> Result<BTreeMap<String, Cid>, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::GetKeystoreMap { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_conversation_keystore(&self, id: Uuid) -> Result<Keystore, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::GetKeystore { id, response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn set_conversation_keystore_map(
        &self,
        document: BTreeMap<String, Cid>,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RootDocumentCommand::SetKeystore {
                document,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
}

struct RootDocumentTask {
    keypair: Arc<DID>,
    path: Option<PathBuf>,
    ipfs: Ipfs,
    cid: Option<Cid>,
    rx: Receiver<RootDocumentCommand>,
}

impl RootDocumentTask {
    pub async fn run(&mut self) {
        self.migrate().await;

        while let Some(command) = self.rx.next().await {
            match command {
                RootDocumentCommand::Get { response } => {
                    let _ = response.send(self.get_root_document().await);
                }
                RootDocumentCommand::Set { document, response } => {
                    let _ = response.send(self.set_root_document(document).await);
                }
                RootDocumentCommand::Identity { response } => {
                    let _ = response.send(self.identity().await);
                }
                RootDocumentCommand::AddFriend { did, response } => {
                    let _ = response.send(self.add_friend(did).await);
                }
                RootDocumentCommand::RemoveFriend { did, response } => {
                    let _ = response.send(self.remove_friend(did).await);
                }
                RootDocumentCommand::GetFriendList { response } => {
                    let _ = response.send(self.friend_list().await);
                }
                RootDocumentCommand::AddRequest { request, response } => {
                    let _ = response.send(self.add_request(request).await);
                }
                RootDocumentCommand::RemoveRequest { request, response } => {
                    let _ = response.send(self.remove_request(request).await);
                }
                RootDocumentCommand::GetRequestList { response } => {
                    let _ = response.send(self.request_list().await);
                }
                RootDocumentCommand::AddBlock { did, response } => {
                    let _ = response.send(self.block_key(did).await);
                }
                RootDocumentCommand::RemoveBlock { did, response } => {
                    let _ = response.send(self.unblock_key(did).await);
                }
                RootDocumentCommand::GetBlockList { response } => {
                    let _ = response.send(self.block_list().await);
                }
                RootDocumentCommand::AddBlockBy { did, response } => {
                    let _ = response.send(self.add_blockby_key(did).await);
                }
                RootDocumentCommand::RemoveBlockBy { did, response } => {
                    let _ = response.send(self.remove_blockby_key(did).await);
                }
                RootDocumentCommand::GetBlockByList { response } => {
                    let _ = response.send(self.blockby_list().await);
                }
                RootDocumentCommand::SetKeystore { document, response } => {
                    let _ = response.send(self.set_conversation_keystore(document).await);
                }
                RootDocumentCommand::GetKeystore { id, response } => {
                    let _ = response.send(self.get_conversation_keystore(id).await);
                }
                RootDocumentCommand::GetKeystoreMap { response } => {
                    let _ = response.send(self.get_conversation_keystore_map().await);
                }
                RootDocumentCommand::Export { response } => {
                    let _ = response.send(self.export().await);
                }
                RootDocumentCommand::ExportEncrypted { response } => {
                    let _ = response.send(self.export_bytes().await);
                }
                RootDocumentCommand::SetIdentityStatus { status, response } => {
                    let _ = response.send(self.set_identity_status(status).await);
                }
                RootDocumentCommand::RootCid { response } => {
                    let _ = response.send(self.cid.ok_or(Error::IdentityInvalid));
                }
                RootDocumentCommand::SetRootCid { cid, response } => {
                    let _ = response.send(self.set_root_cid(cid).await);
                }
            }
        }
    }

    async fn migrate(&mut self) {
        let mut root = match self.get_root_document().await {
            Ok(r) => r,
            Err(_) => return,
        };

        #[derive(serde::Serialize, serde::Deserialize)]
        enum OldRequest {
            In(DID),
            Out(DID),
        }

        let Some(cid) = root.request else {
            return;
        };

        let list = self
            .ipfs
            .get_dag(cid)
            .local()
            .deserialized::<Vec<OldRequest>>()
            .await
            .unwrap_or_default();

        if list.is_empty() {
            return;
        }

        let list = list
            .iter()
            .map(|item| match item {
                OldRequest::In(did) => Request::In {
                    did: did.clone(),
                    date: Utc::now(),
                },
                OldRequest::Out(did) => Request::Out {
                    did: did.clone(),
                    date: Utc::now(),
                },
            })
            .collect::<Vec<_>>();

        let new_cid_fut = async { self.ipfs.dag().put().serialize(list)?.await };

        let new_cid = match new_cid_fut.await {
            Ok(cid) => cid,
            Err(_) => return,
        };

        root.request = Some(new_cid);

        _ = self.set_root_document(root).await;
    }
}

impl RootDocumentTask {
    async fn get_root_document(&self) -> Result<RootDocument, Error> {
        let document: RootDocument = match self.cid {
            Some(cid) => self.ipfs.get_dag(cid).local().deserialized().await?,
            None => return Err(Error::Other),
        };

        document.verify(&self.ipfs).await?;

        Ok(document)
    }

    async fn identity(&self) -> Result<IdentityDocument, Error> {
        let root = self.get_root_document().await?;
        let document: IdentityDocument = self
            .ipfs
            .get_dag(root.identity)
            .local()
            .deserialized()
            .await?;
        document.verify()?;

        Ok(document)
    }

    async fn set_root_document(&mut self, document: RootDocument) -> Result<(), Error> {
        let document = document.sign(&self.keypair)?;

        //Precautionary check
        document.verify(&self.ipfs).await?;

        let root_cid = self.ipfs.dag().put().serialize(document)?.pin(true).await?;

        let old_cid = self.cid.replace(root_cid);

        if let Some(path) = self.path.as_ref() {
            let cid = root_cid.to_string();
            if let Err(e) = tokio::fs::write(path.join(".id"), cid).await {
                tracing::error!("Error writing to '.id': {e}.")
            }
        }

        if let Some(old_cid) = old_cid {
            if old_cid != root_cid && self.ipfs.is_pinned(&old_cid).await.unwrap_or_default() {
                if let Err(e) = self.ipfs.remove_pin(&old_cid).recursive().await {
                    tracing::warn!(cid =? old_cid, "Failed to unpin root document: {e}");
                }
            }
        }

        Ok(())
    }

    async fn set_identity_status(&mut self, status: IdentityStatus) -> Result<(), Error> {
        let mut root = self.get_root_document().await?;
        let mut identity = self.identity().await?;
        identity.metadata.status = Some(status);
        let identity = identity.sign(&self.keypair)?;
        root.identity = self.ipfs.dag().put().serialize(identity)?.await?;

        self.set_root_document(root).await
    }

    async fn request_list(&self) -> Result<Vec<Request>, Error> {
        let cid = match self.cid {
            Some(cid) => cid,
            None => return Ok(vec![]),
        };
        let path = IpfsPath::from(cid).sub_path("request")?;
        let list: Vec<Request> = self
            .ipfs
            .get_dag(path)
            .local()
            .deserialized()
            .await
            .unwrap_or_default();
        Ok(list)
    }

    async fn add_request(&mut self, request: Request) -> Result<(), Error> {
        let mut document = self.get_root_document().await?;
        let old_document = document.request;
        let mut list: Vec<Request> = match document.request {
            Some(cid) => self
                .ipfs
                .get_dag(cid)
                .local()
                .deserialized()
                .await
                .unwrap_or_default(),
            None => vec![],
        };

        if !list.insert_item(&request) {
            return Err(Error::FriendRequestExist);
        }

        document.request =
            (!list.is_empty()).then_some(self.ipfs.dag().put().serialize(list)?.await?);

        self.set_root_document(document).await?;

        if let Some(cid) = old_document {
            if !self.ipfs.is_pinned(&cid).await? {
                self.ipfs.remove_block(cid, false).await?;
            }
        }

        Ok(())
    }

    async fn remove_request(&mut self, request: Request) -> Result<(), Error> {
        let mut document = self.get_root_document().await?;
        let old_document = document.request;
        let mut list: Vec<Request> = match document.request {
            Some(cid) => self
                .ipfs
                .get_dag(cid)
                .local()
                .deserialized()
                .await
                .unwrap_or_default(),
            None => vec![],
        };

        if !list.remove_item(&request) {
            return Err(Error::FriendRequestExist);
        }

        document.request =
            (!list.is_empty()).then_some(self.ipfs.dag().put().serialize(list)?.await?);

        self.set_root_document(document).await?;

        if let Some(cid) = old_document {
            if !self.ipfs.is_pinned(&cid).await? {
                self.ipfs.remove_block(cid, false).await?;
            }
        }

        Ok(())
    }

    async fn friend_list(&self) -> Result<Vec<DID>, Error> {
        let cid = match self.cid {
            Some(cid) => cid,
            None => return Ok(vec![]),
        };
        let path = IpfsPath::from(cid).sub_path("friends")?;
        let list: Vec<DID> = self
            .ipfs
            .get_dag(path)
            .local()
            .deserialized()
            .await
            .unwrap_or_default();
        Ok(list)
    }

    async fn add_friend(&mut self, did: DID) -> Result<(), Error> {
        let mut document = self.get_root_document().await?;
        let old_document = document.friends;
        let mut list: Vec<DID> = match document.friends {
            Some(cid) => self
                .ipfs
                .get_dag(cid)
                .local()
                .deserialized()
                .await
                .unwrap_or_default(),
            None => vec![],
        };

        if !list.insert_item(&did) {
            return Err::<_, Error>(Error::FriendExist);
        }

        document.friends =
            (!list.is_empty()).then_some(self.ipfs.dag().put().serialize(list)?.await?);

        self.set_root_document(document).await?;

        if let Some(cid) = old_document {
            if !self.ipfs.is_pinned(&cid).await? {
                self.ipfs.remove_block(cid, false).await?;
            }
        }

        Ok(())
    }

    async fn remove_friend(&mut self, did: DID) -> Result<(), Error> {
        let mut document = self.get_root_document().await?;
        let old_document = document.friends;
        let mut list: Vec<DID> = match document.friends {
            Some(cid) => self
                .ipfs
                .get_dag(cid)
                .local()
                .deserialized()
                .await
                .unwrap_or_default(),
            None => vec![],
        };

        if !list.remove_item(&did) {
            return Err::<_, Error>(Error::FriendDoesntExist);
        }

        document.friends =
            (!list.is_empty()).then_some(self.ipfs.dag().put().serialize(list)?.await?);

        self.set_root_document(document).await?;

        if let Some(cid) = old_document {
            if !self.ipfs.is_pinned(&cid).await? {
                self.ipfs.remove_block(cid, false).await?;
            }
        }

        Ok(())
    }

    async fn block_list(&self) -> Result<Vec<DID>, Error> {
        let cid = match self.cid {
            Some(cid) => cid,
            None => return Ok(vec![]),
        };
        let path = IpfsPath::from(cid).sub_path("blocks")?;
        let list: Vec<DID> = self
            .ipfs
            .get_dag(path)
            .local()
            .deserialized()
            .await
            .unwrap_or_default();
        Ok(list)
    }

    async fn block_key(&mut self, did: DID) -> Result<(), Error> {
        let mut document = self.get_root_document().await?;
        let old_document = document.blocks;
        let mut list: Vec<DID> = match document.blocks {
            Some(cid) => self
                .ipfs
                .get_dag(cid)
                .local()
                .deserialized()
                .await
                .unwrap_or_default(),
            None => vec![],
        };

        if !list.insert_item(&did) {
            return Err::<_, Error>(Error::PublicKeyIsBlocked);
        }

        document.blocks =
            (!list.is_empty()).then_some(self.ipfs.dag().put().serialize(list)?.await?);

        self.set_root_document(document).await?;

        if let Some(cid) = old_document {
            if !self.ipfs.is_pinned(&cid).await? {
                self.ipfs.remove_block(cid, false).await?;
            }
        }
        Ok(())
    }

    async fn unblock_key(&mut self, did: DID) -> Result<(), Error> {
        let mut document = self.get_root_document().await?;
        let old_document = document.blocks;
        let mut list: Vec<DID> = match document.blocks {
            Some(cid) => self
                .ipfs
                .get_dag(cid)
                .local()
                .deserialized()
                .await
                .unwrap_or_default(),
            None => vec![],
        };

        if !list.remove_item(&did) {
            return Err::<_, Error>(Error::PublicKeyIsntBlocked);
        }

        document.blocks =
            (!list.is_empty()).then_some(self.ipfs.dag().put().serialize(list)?.await?);

        self.set_root_document(document).await?;

        if let Some(cid) = old_document {
            if !self.ipfs.is_pinned(&cid).await? {
                self.ipfs.remove_block(cid, false).await?;
            }
        }
        Ok(())
    }

    async fn blockby_list(&self) -> Result<Vec<DID>, Error> {
        let cid = match self.cid {
            Some(cid) => cid,
            None => return Ok(vec![]),
        };
        let path = IpfsPath::from(cid).sub_path("block_by")?;
        let list: Vec<DID> = self
            .ipfs
            .get_dag(path)
            .local()
            .deserialized()
            .await
            .unwrap_or_default();
        Ok(list)
    }

    async fn add_blockby_key(&mut self, did: DID) -> Result<(), Error> {
        let mut document = self.get_root_document().await?;
        let old_document = document.block_by;
        let mut list: Vec<DID> = match document.block_by {
            Some(cid) => self
                .ipfs
                .get_dag(cid)
                .local()
                .deserialized()
                .await
                .unwrap_or_default(),
            None => vec![],
        };

        if !list.insert_item(&did) {
            return Err::<_, Error>(Error::PublicKeyIsntBlocked);
        }

        document.block_by =
            (!list.is_empty()).then_some(self.ipfs.dag().put().serialize(list)?.await?);

        self.set_root_document(document).await?;

        if let Some(cid) = old_document {
            if !self.ipfs.is_pinned(&cid).await? {
                self.ipfs.remove_block(cid, false).await?;
            }
        }
        Ok(())
    }

    async fn remove_blockby_key(&mut self, did: DID) -> Result<(), Error> {
        let mut document = self.get_root_document().await?;
        let old_document = document.block_by;
        let mut list: Vec<DID> = match document.block_by {
            Some(cid) => self
                .ipfs
                .get_dag(cid)
                .local()
                .deserialized()
                .await
                .unwrap_or_default(),
            None => vec![],
        };

        if !list.remove_item(&did) {
            return Err::<_, Error>(Error::PublicKeyIsntBlocked);
        }

        document.block_by =
            (!list.is_empty()).then_some(self.ipfs.dag().put().serialize(list)?.await?);

        self.set_root_document(document).await?;

        if let Some(cid) = old_document {
            if !self.ipfs.is_pinned(&cid).await? {
                self.ipfs.remove_block(cid, false).await?;
            }
        }
        Ok(())
    }

    async fn set_conversation_keystore(&mut self, map: BTreeMap<String, Cid>) -> Result<(), Error> {
        let mut document = self.get_root_document().await?;
        document.conversations_keystore = Some(self.ipfs.dag().put().serialize(map)?.await?);
        self.set_root_document(document).await
    }

    async fn get_conversation_keystore_map(&self) -> Result<BTreeMap<String, Cid>, Error> {
        let document = self.get_root_document().await?;

        let cid = match document.conversations_keystore {
            Some(cid) => cid,
            None => return Ok(BTreeMap::new()),
        };

        self.ipfs
            .get_dag(cid)
            .local()
            .deserialized()
            .await
            .map_err(Error::from)
    }

    async fn get_conversation_keystore(&self, id: Uuid) -> Result<Keystore, Error> {
        let document = self.get_root_document().await?;

        let cid = match document.conversations_keystore {
            Some(cid) => cid,
            None => return Ok(Keystore::new(id)),
        };

        let path = IpfsPath::from(cid).sub_path(&id.to_string())?;
        self.ipfs
            .get_dag(path)
            .local()
            .deserialized()
            .await
            .map_err(Error::from)
    }

    async fn export(&self) -> Result<ExtractedRootDocument, Error> {
        let document = self.get_root_document().await?;
        document.resolve(&self.ipfs).await
    }

    async fn export_bytes(&self) -> Result<Vec<u8>, Error> {
        let export = self.export().await?;

        let bytes = serde_json::to_vec(&export)?;
        ecdh_encrypt(&self.keypair, None, bytes)
    }

    async fn set_root_cid(&mut self, cid: Cid) -> Result<(), Error> {
        let root_document = self
            .ipfs
            .get_dag(cid)
            .deserialized::<RootDocument>()
            .await?;
        // Step down through each field to resolve it
        root_document.resolve2(&self.ipfs).await?;
        self.set_root_document(root_document).await?;
        Ok(())
    }
}
