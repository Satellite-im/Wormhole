use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use futures::{
    channel::{mpsc, oneshot},
    stream::FuturesUnordered,
    FutureExt, SinkExt, StreamExt,
};
use libipld::Cid;
use rust_ipfs::{Ipfs, IpfsPath};
use uuid::Uuid;
use warp::{
    crypto::DID,
    error::Error,
    raygun::{ConversationType, MessageEventKind},
};

use crate::store::conversation::ConversationDocument;

use super::{utils::GetLocalDag, ToCid};

enum ConversationCommand {
    GetDocument {
        id: Uuid,
        response: oneshot::Sender<Result<ConversationDocument, Error>>,
    },
    SetDocument {
        document: ConversationDocument,
        response: oneshot::Sender<Result<(), Error>>,
    },
    Delete {
        id: Uuid,
        response: oneshot::Sender<Result<ConversationDocument, Error>>,
    },
    Contains {
        id: Uuid,
        response: oneshot::Sender<Result<bool, Error>>,
    },
    List {
        response: oneshot::Sender<Result<Vec<ConversationDocument>, Error>>,
    },
    Subscribe {
        id: Uuid,
        response: oneshot::Sender<Result<tokio::sync::broadcast::Sender<MessageEventKind>, Error>>,
    },
}

#[derive(Debug, Clone)]
pub struct Conversations {
    tx: mpsc::Sender<ConversationCommand>,
    task: Arc<tokio::task::JoinHandle<()>>,
}

impl Drop for Conversations {
    fn drop(&mut self) {
        if Arc::strong_count(&self.task) == 1 && !self.task.is_finished() {
            self.task.abort();
        }
    }
}

impl Conversations {
    pub async fn new(ipfs: &Ipfs, path: Option<PathBuf>, keypair: Arc<DID>) -> Self {
        let cid = match path.as_ref() {
            Some(path) => tokio::fs::read(path.join(".message_id"))
                .await
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                .ok()
                .and_then(|cid_str| cid_str.parse().ok()),
            None => None,
        };

        let (tx, rx) = futures::channel::mpsc::channel(1);

        let mut task = ConversationTask {
            ipfs: ipfs.clone(),
            event_handler: Default::default(),
            keypair,
            path,
            cid,
            rx,
        };

        let handle = tokio::spawn(async move {
            task.start().await;
        });

        Self {
            tx,
            task: Arc::new(handle),
        }
    }

    pub async fn get(&self, id: Uuid) -> Result<ConversationDocument, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(ConversationCommand::GetDocument { id, response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn contains(&self, id: Uuid) -> Result<bool, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(ConversationCommand::Contains { id, response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn set(&self, document: ConversationDocument) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(ConversationCommand::SetDocument {
                document,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn delete(&self, id: Uuid) -> Result<ConversationDocument, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(ConversationCommand::Delete { id, response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn list(&self) -> Result<Vec<ConversationDocument>, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(ConversationCommand::List { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn subscribe(
        &self,
        id: Uuid,
    ) -> Result<tokio::sync::broadcast::Sender<MessageEventKind>, Error> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(ConversationCommand::Subscribe { id, response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
}

struct ConversationTask {
    ipfs: Ipfs,
    cid: Option<Cid>,
    path: Option<PathBuf>,
    keypair: Arc<DID>,
    event_handler: HashMap<Uuid, tokio::sync::broadcast::Sender<MessageEventKind>>,
    rx: mpsc::Receiver<ConversationCommand>,
}

impl ConversationTask {
    async fn start(&mut self) {
        while let Some(command) = self.rx.next().await {
            match command {
                ConversationCommand::GetDocument { id, response } => {
                    let _ = response.send(self.get(id).await);
                }
                ConversationCommand::SetDocument { document, response } => {
                    let _ = response.send(self.set_document(document).await);
                }
                ConversationCommand::List { response } => {
                    let _ = response.send(self.list().await);
                }
                ConversationCommand::Delete { id, response } => {
                    let _ = response.send(self.delete(id).await);
                }
                ConversationCommand::Subscribe { id, response } => {
                    let _ = response.send(self.subscribe(id).await);
                }
                ConversationCommand::Contains { id, response } => {
                    let _ = response.send(Ok(self.contains(id).await));
                }
            }
        }
    }

    async fn get(&self, id: Uuid) -> Result<ConversationDocument, Error> {
        let cid = match self.cid {
            Some(cid) => cid,
            None => return Err(Error::InvalidConversation),
        };

        let path = IpfsPath::from(cid).sub_path(&id.to_string())?;

        let document: ConversationDocument = path.get_local_dag(&self.ipfs).await?;
        document.verify()?;
        Ok(document)
    }

    async fn delete(&mut self, id: Uuid) -> Result<ConversationDocument, Error> {
        let cid = match self.cid {
            Some(cid) => cid,
            None => return Err(Error::InvalidConversation),
        };

        let mut conversation_map: BTreeMap<String, Cid> = cid.get_local_dag(&self.ipfs).await?;

        let document_cid = match conversation_map.remove(&id.to_string()) {
            Some(cid) => cid,
            None => return Err(Error::InvalidConversation),
        };

        self.set_map(conversation_map).await?;

        let document: ConversationDocument = document_cid.get_local_dag(&self.ipfs).await?;
        Ok(document)
    }

    async fn list(&self) -> Result<Vec<ConversationDocument>, Error> {
        let cid = match self.cid {
            Some(cid) => cid,
            None => return Ok(Vec::new()),
        };

        let conversation_map: BTreeMap<String, Cid> = cid.get_local_dag(&self.ipfs).await?;

        let list = FuturesUnordered::from_iter(
            conversation_map
                .values()
                .map(|cid| (*cid).get_local_dag(&self.ipfs).boxed()),
        )
        .filter_map(|result: Result<ConversationDocument, _>| async move { result.ok() })
        .collect::<Vec<_>>()
        .await;

        Ok(list)
    }

    async fn contains(&self, id: Uuid) -> bool {
        let cid = match self.cid {
            Some(cid) => cid,
            None => return false,
        };

        let conversation_map: BTreeMap<String, Cid> = match cid.get_local_dag(&self.ipfs).await {
            Ok(document) => document,
            Err(_) => return false,
        };

        conversation_map.contains_key(&id.to_string())
    }

    async fn set_map(&mut self, map: BTreeMap<String, Cid>) -> Result<(), Error> {
        let cid = map.to_cid(&self.ipfs).await?;

        let old_map_cid = self.cid.replace(cid);

        self.ipfs.insert_pin(&cid, true).await?;

        if let Some(old_cid) = old_map_cid {
            if self.ipfs.is_pinned(&old_cid).await.unwrap_or_default() {
                self.ipfs.remove_pin(&old_cid, true).await?;
            }
        }

        self.cid = Some(cid);

        if let Some(path) = self.path.as_ref() {
            let cid = cid.to_string();
            if let Err(e) = tokio::fs::write(path.join(".message_id"), cid).await {
                tracing::log::error!("Error writing to '.message_id': {e}.")
            }
        }

        Ok(())
    }

    async fn set_document(&mut self, mut document: ConversationDocument) -> Result<(), Error> {
        if let Some(creator) = document.creator.as_ref() {
            if creator.eq(&self.keypair)
                && matches!(document.conversation_type, ConversationType::Group)
            {
                document.sign(&self.keypair)?;
            }
        }

        document.verify()?;

        let mut map = match self.cid {
            Some(cid) => {
                let map = cid.get_local_dag(&self.ipfs).await?;
                map
            }
            None => BTreeMap::new(),
        };

        let id = document.id().to_string();
        let cid = document.to_cid(&self.ipfs).await?;

        map.insert(id, cid);

        self.set_map(map).await
    }

    async fn subscribe(
        &mut self,
        id: Uuid,
    ) -> Result<tokio::sync::broadcast::Sender<MessageEventKind>, Error> {
        if !self.contains(id).await {
            return Err(Error::InvalidConversation);
        }

        if let Some(tx) = self.event_handler.get(&id) {
            return Ok(tx.clone());
        }

        let (tx, _) = tokio::sync::broadcast::channel(1024);

        self.event_handler.insert(id, tx.clone());

        Ok(tx)
    }
}
