mod attachment;
mod community_task;
mod task;

use community_task::CommunityTaskCommand;
use futures_timer::Delay;
use task::ConversationTaskCommand;

use async_rt::AbortableJoinHandle;
use bytes::Bytes;
use std::borrow::BorrowMut;
use std::path::PathBuf;
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};
use web_time::Instant;

use futures::{
    channel::{mpsc, oneshot},
    pin_mut,
    stream::BoxStream,
    SinkExt, Stream, StreamExt, TryFutureExt,
};
use indexmap::{IndexMap, IndexSet};
use ipld_core::cid::Cid;

use rust_ipfs::{Ipfs, PeerId};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::community::CommunityInviteDocument;
use super::topics::ConversationTopic;
use super::{document::root::RootDocumentMap, ds_key::DataStoreKey, PeerIdExt};
use crate::store::CommunityJoinEvents;
use crate::store::{
    conversation::ConversationDocument,
    discovery::Discovery,
    event_subscription::EventSubscription,
    files::FileStore,
    generate_shared_topic,
    identity::IdentityStore,
    keystore::Keystore,
    payload::{PayloadBuilder, PayloadMessage},
    sign_serde,
    topics::PeerTopic,
    ConversationEvents, ConversationRequestKind, ConversationRequestResponse, DidExt,
};

use crate::store::community::CommunityDocument;
use chrono::{DateTime, Utc};
use warp::raygun::community::{
    Community, CommunityChannel, CommunityChannelType, CommunityInvite, CommunityRole, RoleId,
};
use warp::raygun::{ConversationImage, GroupPermissionOpt, Message};
use warp::{
    constellation::ConstellationProgressStream,
    crypto::DID,
    error::Error,
    multipass::MultiPassEventKind,
    raygun::{
        AttachmentEventStream, Conversation, ConversationType, Location, MessageEvent,
        MessageEventKind, MessageOptions, MessageReference, MessageStatus, Messages, PinState,
        RayGunEventKind, ReactionState,
    },
};

const CHAT_DIRECTORY: &str = "chat_media";

pub type DownloadStream = BoxStream<'static, Result<Bytes, std::io::Error>>;

#[derive(Clone)]
pub struct MessageStore {
    inner: Arc<tokio::sync::RwLock<ConversationInner>>,
    _handle: AbortableJoinHandle<()>,
}

impl MessageStore {
    pub async fn new(
        ipfs: &Ipfs,
        discovery: Discovery,
        file: &FileStore,
        event: EventSubscription<RayGunEventKind>,
        identity: &IdentityStore,
    ) -> Self {
        tracing::info!("Initializing MessageStore");

        let root = identity.root_document().clone();

        let mut inner = ConversationInner {
            ipfs: ipfs.clone(),
            conversation_task: HashMap::new(),
            community_task: HashMap::new(),
            community_invites: vec![],
            identity: identity.clone(),
            root,
            discovery,
            file: file.clone(),
            event,
            queue: Default::default(),
        };

        if let Err(e) = inner.migrate().await {
            tracing::warn!(error = %e, "unable to migrate conversations to root document");
        }

        inner.load_conversations().await;

        let inner = Arc::new(tokio::sync::RwLock::new(inner));

        let task = ConversationTask {
            inner: inner.clone(),
            ipfs: ipfs.clone(),
            identity: identity.clone(),
        };

        let _handle = async_rt::task::spawn_abortable(task.run());

        Self { inner, _handle }
    }
}

impl MessageStore {
    pub async fn get_conversation(&self, id: Uuid) -> Result<Conversation, Error> {
        let document = self.get(id).await?;
        Ok(document.into())
    }

    pub async fn list_conversations(&self) -> Result<Vec<Conversation>, Error> {
        self.list()
            .await
            .map(|list| list.into_iter().map(|document| document.into()).collect())
    }

    pub async fn get_conversation_stream(
        &self,
        conversation_id: Uuid,
    ) -> Result<impl Stream<Item = MessageEventKind>, Error> {
        let mut rx = self.subscribe(conversation_id).await?.subscribe();
        Ok(async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(event) => yield event,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(_) => {}
                };
            }
        })
    }

    pub async fn get_community_stream(
        &self,
        community_id: Uuid,
    ) -> Result<impl Stream<Item = MessageEventKind>, Error> {
        let mut rx = self.subscribe_community(community_id).await?.subscribe();
        Ok(async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(event) => yield event,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(_) => {}
                };
            }
        })
    }

    pub async fn get(&self, id: Uuid) -> Result<ConversationDocument, Error> {
        let inner = &*self.inner.read().await;
        inner.get(id).await
    }

    pub async fn get_keystore(&self, id: Uuid) -> Result<Keystore, Error> {
        let inner = &*self.inner.read().await;
        inner.get_keystore(id).await
    }

    pub async fn contains(&self, id: Uuid) -> Result<bool, Error> {
        let inner = &*self.inner.read().await;
        Ok(inner.contains(id).await)
    }

    pub async fn set<B: BorrowMut<ConversationDocument>>(&self, document: B) -> Result<(), Error> {
        let inner = &mut *self.inner.write().await;
        inner.set_document(document).await
    }

    pub async fn delete(&self, id: Uuid) -> Result<ConversationDocument, Error> {
        let inner = &mut *self.inner.write().await;
        inner.delete(id).await
    }

    pub async fn list(&self) -> Result<Vec<ConversationDocument>, Error> {
        let inner = &*self.inner.read().await;
        Ok(inner.list().await)
    }

    pub async fn subscribe(
        &self,
        id: Uuid,
    ) -> Result<tokio::sync::broadcast::Sender<MessageEventKind>, Error> {
        let inner = &mut *self.inner.write().await;
        inner.subscribe(id).await
    }

    pub async fn subscribe_community(
        &self,
        id: Uuid,
    ) -> Result<tokio::sync::broadcast::Sender<MessageEventKind>, Error> {
        let inner = &mut *self.inner.write().await;
        inner.subscribe_community(id).await
    }

    pub async fn create_conversation(&self, did: &DID) -> Result<Conversation, Error> {
        let inner = &mut *self.inner.write().await;
        inner.create_conversation(did).await
    }

    pub async fn create_group_conversation<P: Into<GroupPermissionOpt> + Send + Sync>(
        &self,
        name: Option<String>,
        members: HashSet<DID>,
        permissions: P,
    ) -> Result<Conversation, Error> {
        let inner = &mut *self.inner.write().await;
        inner
            .create_group_conversation(name, members, permissions)
            .await
    }

    pub async fn set_favorite_conversation(
        &self,
        conversation_id: Uuid,
        favorite: bool,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::FavoriteConversation {
                favorite,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_message(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<warp::raygun::Message, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::GetMessage {
                message_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_messages(
        &self,
        conversation_id: Uuid,
        opt: MessageOptions,
    ) -> Result<Messages, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::GetMessages {
                options: opt,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn messages_count(&self, conversation_id: Uuid) -> Result<usize, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::GetMessagesCount { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_message_reference(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageReference, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::GetMessageReference {
                message_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_message_references(
        &self,
        conversation_id: Uuid,
        opt: MessageOptions,
    ) -> Result<BoxStream<'static, MessageReference>, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::GetMessageReferences {
                options: opt,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn update_conversation_name(
        &self,
        conversation_id: Uuid,
        name: &str,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::UpdateConversationName {
                name: name.to_string(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn update_conversation_permissions<P: Into<GroupPermissionOpt> + Send + Sync>(
        &self,
        conversation_id: Uuid,
        permissions: P,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::UpdateConversationPermissions {
                permissions: permissions.into(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn delete_conversation(&self, conversation_id: Uuid) -> Result<(), Error> {
        let inner = &mut *self.inner.write().await;
        inner.delete_conversation(conversation_id, true).await
    }

    pub async fn add_participant(&self, conversation_id: Uuid, did: &DID) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::AddParticipant {
                member: did.clone(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn remove_participant(&self, conversation_id: Uuid, did: &DID) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::RemoveParticipant {
                member: did.clone(),
                broadcast: true,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn message_status(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageStatus, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::MessageStatus {
                message_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn send_message(
        &self,
        conversation_id: Uuid,
        lines: Vec<String>,
    ) -> Result<Uuid, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::SendMessage {
                lines,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn edit_message(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
        lines: Vec<String>,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::EditMessage {
                message_id,
                lines,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn reply(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
        lines: Vec<String>,
    ) -> Result<Uuid, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::ReplyMessage {
                message_id,
                lines,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn delete_message(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::DeleteMessage {
                message_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn pin_message(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
        state: PinState,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::PinMessage {
                message_id,
                state,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn react(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::ReactMessage {
                message_id,
                state,
                emoji,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn attach(
        &self,
        conversation_id: Uuid,
        message_id: Option<Uuid>,
        locations: Vec<Location>,
        messages: Vec<String>,
    ) -> Result<(Uuid, AttachmentEventStream), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::AttachMessage {
                message_id,
                locations,
                lines: messages,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn download<P: AsRef<Path>>(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
        file: &str,
        path: P,
    ) -> Result<ConstellationProgressStream, Error> {
        let path = path.as_ref().to_path_buf();
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::DownloadAttachment {
                message_id,
                file: file.to_owned(),
                path,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn download_stream(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
        file: &str,
    ) -> Result<DownloadStream, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::DownloadAttachmentStream {
                message_id,
                file: file.to_owned(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn send_event(
        &self,
        conversation_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::SendEvent {
                event,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn cancel_event(
        &self,
        conversation_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::CancelEvent {
                event,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn update_conversation_icon(
        &self,
        conversation_id: Uuid,
        location: Location,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::UpdateIcon {
                location,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn update_conversation_banner(
        &self,
        conversation_id: Uuid,
        location: Location,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::UpdateBanner {
                location,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn conversation_icon(
        &mut self,
        conversation_id: Uuid,
    ) -> Result<ConversationImage, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::GetIcon { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn conversation_banner(
        &mut self,
        conversation_id: Uuid,
    ) -> Result<ConversationImage, Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::GetBanner { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn remove_conversation_icon(&self, conversation_id: Uuid) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::RemoveIcon { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn remove_conversation_banner(&self, conversation_id: Uuid) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::RemoveBanner { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn set_description(
        &self,
        conversation_id: Uuid,
        desc: Option<&str>,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::SetDescription {
                desc: desc.map(|s| s.to_string()),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn archived_conversation(&self, conversation_id: Uuid) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::ArchivedConversation { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn unarchived_conversation(&self, conversation_id: Uuid) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let conversation_meta = inner
            .conversation_task
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let (tx, rx) = oneshot::channel();
        let _ = conversation_meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::UnarchivedConversation { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
}

impl MessageStore {
    pub async fn create_community(&mut self, name: &str) -> Result<Community, Error> {
        let inner = &mut *self.inner.write().await;
        inner.create_community(name).await
    }
    pub async fn delete_community(&mut self, community_id: Uuid) -> Result<(), Error> {
        let inner = &mut *self.inner.write().await;
        inner.delete_community(community_id).await
    }
    pub async fn get_community(&mut self, community_id: Uuid) -> Result<Community, Error> {
        let inner = &mut *self.inner.write().await;
        inner.get_community(community_id).await
    }

    pub async fn list_communities_joined(&self) -> Result<IndexSet<Uuid>, Error> {
        let inner = &mut *self.inner.write().await;
        inner.list_communities_joined().await
    }
    pub async fn list_communities_invited_to(&self) -> Result<Vec<(Uuid, CommunityInvite)>, Error> {
        let inner = &*self.inner.read().await;
        Ok(inner
            .community_invites
            .iter()
            .map(|(community_id, i)| (*community_id, CommunityInvite::from(i.clone())))
            .collect())
    }
    pub async fn leave_community(&mut self, community_id: Uuid) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::LeaveCommunity { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_community_icon(&self, community_id: Uuid) -> Result<ConversationImage, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityIcon { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn get_community_banner(
        &self,
        community_id: Uuid,
    ) -> Result<ConversationImage, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityBanner { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn edit_community_icon(
        &mut self,
        community_id: Uuid,
        location: Location,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EditCommunityIcon {
                location,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn edit_community_banner(
        &mut self,
        community_id: Uuid,
        location: Location,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EditCommunityBanner {
                location,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn create_community_invite(
        &mut self,
        community_id: Uuid,
        target_user: Option<DID>,
        expiry: Option<DateTime<Utc>>,
    ) -> Result<CommunityInvite, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::CreateCommunityInvite {
                target_user,
                expiry,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn delete_community_invite(
        &mut self,
        community_id: Uuid,
        invite_id: Uuid,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        match inner.community_task.get(&community_id) {
            None => {
                let keypair = inner.root.keypair();

                let event = CommunityJoinEvents::DeleteInvite { invite_id };
                let payload = PayloadBuilder::new(keypair, event)
                    .from_ipfs(&inner.ipfs)
                    .await?;
                let bytes = payload.to_bytes()?;

                if let Err(e) = inner
                    .ipfs
                    .pubsub_publish(community_id.join_topic(), bytes)
                    .await
                {
                    tracing::error!(id=%community_id, "Unable to send event: {e}");
                }
            }
            Some(community_meta) => {
                let (tx, rx) = oneshot::channel();
                let _ = community_meta
                    .command_tx
                    .clone()
                    .send(CommunityTaskCommand::DeleteCommunityInvite {
                        invite_id,
                        response: tx,
                    })
                    .await;
                return rx.await.map_err(anyhow::Error::from)?;
            }
        }
        Ok(())
    }
    pub async fn get_community_invite(
        &mut self,
        community_id: Uuid,
        invite_id: Uuid,
    ) -> Result<CommunityInvite, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityInvite {
                invite_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn request_join_community(&mut self, community_id: Uuid) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let keypair = inner.root.keypair();

        let event = CommunityJoinEvents::Join;
        let payload = PayloadBuilder::new(keypair, event)
            .from_ipfs(&inner.ipfs)
            .await?;
        let bytes = payload.to_bytes()?;

        if let Err(e) = inner
            .ipfs
            .pubsub_publish(community_id.join_topic(), bytes)
            .await
        {
            tracing::error!(id=%community_id, "Unable to send event: {e}");
        }
        Ok(())
    }
    pub async fn edit_community_invite(
        &mut self,
        community_id: Uuid,
        invite_id: Uuid,
        invite: CommunityInvite,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EditCommunityInvite {
                invite_id,
                invite,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn create_community_role(
        &mut self,
        community_id: Uuid,
        name: &str,
    ) -> Result<CommunityRole, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::CreateCommunityRole {
                name: name.to_string(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn delete_community_role(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::DeleteCommunityRole {
                role_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn get_community_role(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
    ) -> Result<CommunityRole, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityRole {
                role_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn edit_community_role_name(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
        new_name: String,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EditCommunityRoleName {
                role_id,
                new_name,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn grant_community_role(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
        user: DID,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GrantCommunityRole {
                role_id,
                user,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn revoke_community_role(
        &mut self,
        community_id: Uuid,
        role_id: RoleId,
        user: DID,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::RevokeCommunityRole {
                role_id,
                user,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn create_community_channel(
        &mut self,
        community_id: Uuid,
        channel_name: &str,
        channel_type: CommunityChannelType,
    ) -> Result<CommunityChannel, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::CreateCommunityChannel {
                channel_name: channel_name.to_string(),
                channel_type,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn delete_community_channel(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::DeleteCommunityChannel {
                channel_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn get_community_channel(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
    ) -> Result<CommunityChannel, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityChannel {
                channel_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn edit_community_name(
        &mut self,
        community_id: Uuid,
        name: &str,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EditCommunityName {
                name: name.to_string(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn edit_community_description(
        &mut self,
        community_id: Uuid,
        description: Option<String>,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EditCommunityDescription {
                description,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn grant_community_permission(
        &mut self,
        community_id: Uuid,
        permission: String,
        role_id: RoleId,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GrantCommunityPermission {
                permission,
                role_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn revoke_community_permission(
        &mut self,
        community_id: Uuid,
        permission: String,
        role_id: RoleId,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::RevokeCommunityPermission {
                permission,
                role_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn grant_community_permission_for_all(
        &mut self,
        community_id: Uuid,
        permission: String,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GrantCommunityPermissionForAll {
                permission,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn has_community_permission(
        &mut self,
        community_id: Uuid,
        permission: String,
        member: DID,
    ) -> Result<bool, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::HasCommunityPermission {
                permission,
                member,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn revoke_community_permission_for_all(
        &mut self,
        community_id: Uuid,
        permission: String,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::RevokeCommunityPermissionForAll {
                permission,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn remove_community_member(
        &mut self,
        community_id: Uuid,
        member: DID,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::RemoveCommunityMember {
                member,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn edit_community_channel_name(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        name: &str,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EditCommunityChannelName {
                channel_id,
                name: name.to_string(),
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn edit_community_channel_description(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        description: Option<String>,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EditCommunityChannelDescription {
                channel_id,
                description,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn grant_community_channel_permission(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: String,
        role_id: RoleId,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GrantCommunityChannelPermission {
                channel_id,
                permission,
                role_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn revoke_community_channel_permission(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: String,
        role_id: RoleId,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::RevokeCommunityChannelPermission {
                channel_id,
                permission,
                role_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn grant_community_channel_permission_for_all(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: String,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(
                CommunityTaskCommand::GrantCommunityChannelPermissionForAll {
                    channel_id,
                    permission,
                    response: tx,
                },
            )
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn revoke_community_channel_permission_for_all(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: String,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(
                CommunityTaskCommand::RevokeCommunityChannelPermissionForAll {
                    channel_id,
                    permission,
                    response: tx,
                },
            )
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn has_community_channel_permission(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        permission: String,
        member: DID,
    ) -> Result<bool, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::HasCommunityChannelPermission {
                channel_id,
                permission,
                member,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    pub async fn get_community_channel_message(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<Message, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityChannelMessage {
                channel_id,
                message_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn get_community_channel_messages(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        options: MessageOptions,
    ) -> Result<Messages, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityChannelMessages {
                channel_id,
                options,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn get_community_channel_message_count(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
    ) -> Result<usize, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityChannelMessageCount {
                channel_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn get_community_channel_message_reference(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageReference, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityChannelMessageReference {
                channel_id,
                message_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn get_community_channel_message_references(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        options: MessageOptions,
    ) -> Result<BoxStream<'static, MessageReference>, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::GetCommunityChannelMessageReferences {
                channel_id,
                options,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn community_channel_message_status(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageStatus, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::CommunityChannelMessageStatus {
                channel_id,
                message_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn send_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message: Vec<String>,
    ) -> Result<Uuid, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::SendCommunityChannelMessage {
                channel_id,
                message,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn edit_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        message: Vec<String>,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EditCommunityChannelMessage {
                channel_id,
                message_id,
                message,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn reply_to_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        message: Vec<String>,
    ) -> Result<Uuid, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::ReplyToCommunityChannelMessage {
                channel_id,
                message_id,
                message,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn delete_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::DeleteCommunityChannelMessage {
                channel_id,
                message_id,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn pin_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        state: PinState,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::PinCommunityChannelMessage {
                channel_id,
                message_id,
                state,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn react_to_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::ReactToCommunityChannelMessage {
                channel_id,
                message_id,
                state,
                emoji,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn send_community_channel_messsage_event(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::SendCommunityChannelMesssageEvent {
                channel_id,
                event,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn cancel_community_channel_messsage_event(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::CancelCommunityChannelMesssageEvent {
                channel_id,
                event,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn attach_to_community_channel_message(
        &mut self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Option<Uuid>,
        locations: Vec<Location>,
        message: Vec<String>,
    ) -> Result<(Uuid, AttachmentEventStream), Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::AttachToCommunityChannelMessage {
                channel_id,
                message_id,
                locations,
                message,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn download_from_community_channel_message(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        file: String,
        path: PathBuf,
    ) -> Result<ConstellationProgressStream, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::DownloadFromCommunityChannelMessage {
                channel_id,
                message_id,
                file,
                path,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
    pub async fn download_stream_from_community_channel_message(
        &self,
        community_id: Uuid,
        channel_id: Uuid,
        message_id: Uuid,
        file: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, std::io::Error>>, Error> {
        let inner = &*self.inner.read().await;
        let community_meta = inner
            .community_task
            .get(&community_id)
            .ok_or(Error::InvalidCommunity)?;
        let (tx, rx) = oneshot::channel();
        let _ = community_meta
            .command_tx
            .clone()
            .send(
                CommunityTaskCommand::DownloadStreamFromCommunityChannelMessage {
                    channel_id,
                    message_id,
                    file: file.to_owned(),
                    response: tx,
                },
            )
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }
}

struct ConversationTask {
    inner: Arc<tokio::sync::RwLock<ConversationInner>>,
    ipfs: Ipfs,
    identity: IdentityStore,
}

impl ConversationTask {
    async fn run(self) {
        let mut identity_stream = self
            .identity
            .subscribe()
            .await
            .expect("Channel isnt dropped");

        let stream = self
            .ipfs
            .pubsub_subscribe(self.identity.did_key().messaging())
            .await
            .expect("valid subscription");

        pin_mut!(stream);

        let mut queue_timer = Delay::new(Duration::from_secs(5));

        loop {
            tokio::select! {
                biased;
                Some(ev) = identity_stream.next() => {
                    if let Err(e) = process_identity_events(&mut *self.inner.write().await, ev).await {
                        tracing::error!("Error processing identity events: {e}");
                    }
                }
                Some(message) = stream.next() => {
                    let payload = match PayloadMessage::<ConversationEvents>::from_bytes(&message.data) {
                        Ok(payload) => payload,
                        Err(e) => {
                            tracing::warn!("Failed to parse payload data: {e}");
                            continue;
                        }
                    };

                    let sender_peer_id = payload.sender();

                    let sender = match sender_peer_id.to_did() {
                        Ok(did) => did,
                        Err(e) => {
                            tracing::warn!(sender = %sender_peer_id, error = %e, "unable to convert to did");
                            continue;
                        }
                    };

                    let event = match payload.message(self.identity.root_document().keypair()) {
                        Ok(m) => m,
                        Err(e) => {
                            tracing::error!(%sender, error = %e, "unable to obtain message from payload");
                            continue
                        }
                    };

                    if let Err(e) = process_conversation(&mut *self.inner.write().await, *sender_peer_id, event).await {
                        tracing::error!(%sender, error = %e, "error processing conversation");
                    }
                }
                _ = &mut queue_timer => {
                    let _ = _process_queue(&mut *self.inner.write().await).await;
                    queue_timer.reset(Duration::from_secs(5));
                }


            }
        }
    }
}

#[derive(Clone, Debug)]
struct ConversationInnerMeta {
    pub command_tx: mpsc::Sender<ConversationTaskCommand>,
    pub handle: AbortableJoinHandle<()>,
}
#[derive(Clone)]
struct CommunityInnerMeta {
    pub command_tx: mpsc::Sender<CommunityTaskCommand>,
    pub handle: AbortableJoinHandle<()>,
}

struct ConversationInner {
    ipfs: Ipfs,
    conversation_task: HashMap<Uuid, ConversationInnerMeta>,
    community_task: HashMap<Uuid, CommunityInnerMeta>,
    community_invites: Vec<(Uuid, CommunityInviteDocument)>,
    root: RootDocumentMap,
    file: FileStore,
    event: EventSubscription<RayGunEventKind>,
    identity: IdentityStore,
    discovery: Discovery,

    // Note: Temporary
    queue: HashMap<DID, Vec<Queue>>,
}

impl ConversationInner {
    async fn migrate(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn load_conversations(&mut self) {
        let mut stream = self.list_stream().await;
        while let Some(conversation) = stream.next().await {
            let id = conversation.id();

            if let Err(e) = self.create_conversation_task(id).await {
                tracing::error!(id = %id, error = %e, "Failed to load conversation");
            }
        }

        let ipfs = &self.ipfs;
        let key = ipfs.messaging_queue();

        if let Ok(data) = futures::future::ready(
            ipfs.repo()
                .data_store()
                .get(key.as_bytes())
                .await
                .unwrap_or_default()
                .ok_or(Error::Other),
        )
        .and_then(|bytes| async move {
            let cid_str = String::from_utf8_lossy(&bytes).to_string();

            let cid = cid_str.parse::<Cid>().map_err(anyhow::Error::from)?;

            Ok(cid)
        })
        .and_then(|cid| async move {
            ipfs.get_dag(cid)
                .local()
                .deserialized::<HashMap<_, _>>()
                .await
                .map_err(anyhow::Error::from)
                .map_err(Error::from)
        })
        .await
        {
            self.queue = data;
        }
    }

    async fn create_conversation_task(&mut self, conversation_id: Uuid) -> Result<(), Error> {
        let (ctx, crx) = mpsc::channel(256);

        let task = task::ConversationTask::new(
            conversation_id,
            &self.ipfs,
            &self.root,
            &self.identity,
            &self.file,
            &self.discovery,
            crx,
            self.event.clone(),
        )
        .await?;

        let handle = async_rt::task::spawn_abortable(task.run());

        tracing::info!(%conversation_id, "started conversation");

        let inner_meta = ConversationInnerMeta {
            command_tx: ctx,
            handle,
        };

        self.conversation_task.insert(conversation_id, inner_meta);

        Ok(())
    }

    async fn create_conversation(&mut self, did: &DID) -> Result<Conversation, Error> {
        //TODO: maybe use root document to directly check
        // if self.with_friends.load(Ordering::SeqCst) && !self.identity.is_friend(did_key).await? {
        //     return Err(Error::FriendDoesntExist);
        // }

        if self.root.is_blocked(did).await.unwrap_or_default() {
            return Err(Error::PublicKeyIsBlocked);
        }

        let own_did = self.identity.did_key();

        if did == &own_did {
            return Err(Error::CannotCreateConversation);
        }

        if let Some(conversation) = self
            .list()
            .await
            .iter()
            .find(|conversation| {
                conversation.conversation_type() == ConversationType::Direct
                    && conversation.recipients().contains(did)
                    && conversation.recipients().contains(&own_did)
            })
            .map(Conversation::from)
        {
            return Err(Error::ConversationExist { conversation });
        }

        //Temporary limit
        // if self.list_conversations().await.unwrap_or_default().len() >= 256 {
        //     return Err(Error::ConversationLimitReached);
        // }

        let mut conversation =
            ConversationDocument::new_direct(self.root.keypair(), [own_did.clone(), did.clone()])?;

        let convo_id = conversation.id();

        self.set_document(&mut conversation).await?;

        self.create_conversation_task(convo_id).await?;

        let peer_id = did.to_peer_id()?;

        let event = ConversationEvents::NewConversation {
            recipient: own_did.clone(),
        };

        let payload = PayloadBuilder::new(self.root.keypair(), event)
            .add_recipient(did)?
            .from_ipfs(&self.ipfs)
            .await?;

        let payload_bytes = payload.to_bytes()?;

        let peers = self.ipfs.pubsub_peers(Some(did.messaging())).await?;

        if !peers.contains(&peer_id)
            || (peers.contains(&peer_id)
                && self
                    .ipfs
                    .pubsub_publish(did.messaging(), payload_bytes.clone())
                    .await
                    .is_err())
        {
            tracing::warn!(conversation_id = %convo_id, "Unable to publish to topic. Queuing event");
            self.queue_event(
                did.clone(),
                Queue::direct(peer_id, did.messaging(), payload_bytes.to_vec()),
            )
            .await;
        }

        self.event
            .emit(RayGunEventKind::ConversationCreated {
                conversation_id: convo_id,
            })
            .await;

        Ok(Conversation::from(&conversation))
    }

    pub async fn create_group_conversation<P: Into<GroupPermissionOpt> + Send + Sync>(
        &mut self,
        name: Option<String>,
        mut recipients: HashSet<DID>,
        permissions: P,
    ) -> Result<Conversation, Error> {
        let own_did = &self.identity.did_key();

        if recipients.contains(own_did) {
            return Err(Error::CannotCreateConversation);
        }

        if let Some(name) = name.as_ref() {
            let name_length = name.trim().len();

            if name_length == 0 || name_length > 255 {
                return Err(Error::InvalidLength {
                    context: "name".into(),
                    current: name_length,
                    minimum: Some(1),
                    maximum: Some(255),
                });
            }
        }

        let mut removal = vec![];

        for did in recipients.iter() {
            let is_blocked = self.root.is_blocked(did).await?;
            let is_blocked_by = self.root.is_blocked_by(did).await?;
            if is_blocked || is_blocked_by {
                tracing::info!("{did} is blocked.. removing from list");
                removal.push(did.clone());
            }
        }

        for did in removal {
            recipients.remove(&did);
        }

        //Temporary limit
        // if self.list_conversations().await.unwrap_or_default().len() >= 256 {
        //     return Err(Error::ConversationLimitReached);
        // }

        let restricted = self.root.get_blocks().await.unwrap_or_default();

        let permissions = match permissions.into() {
            GroupPermissionOpt::Map(permissions) => permissions,
            GroupPermissionOpt::Single((id, set)) => IndexMap::from_iter(vec![(id, set)]),
        };

        let mut conversation = ConversationDocument::new_group(
            self.root.keypair(),
            name,
            recipients,
            &restricted,
            permissions,
        )?;

        let recipient = conversation.recipients();

        let conversation_id = conversation.id();

        self.set_document(&mut conversation).await?;

        self.create_conversation_task(conversation_id).await?;

        let peer_id_list = recipient
            .iter()
            .filter(|did| own_did.ne(did))
            .map(|did| (did.clone(), did))
            .filter_map(|(a, b)| b.to_peer_id().map(|pk| (a, pk)).ok())
            .collect::<Vec<_>>();

        let event = ConversationEvents::NewGroupConversation {
            conversation: conversation.clone(),
        };

        let payload = PayloadBuilder::new(self.root.keypair(), event)
            .add_recipients(peer_id_list.iter().map(|(did, _)| did))?
            .from_ipfs(&self.ipfs)
            .await?;

        let payload_bytes = payload.to_bytes()?;

        for (did, peer_id) in peer_id_list {
            let peers = self.ipfs.pubsub_peers(Some(did.messaging())).await?;
            if !peers.contains(&peer_id)
                || (peers.contains(&peer_id)
                    && self
                        .ipfs
                        .pubsub_publish(did.messaging(), payload_bytes.clone())
                        .await
                        .is_err())
            {
                tracing::warn!("Unable to publish to topic. Queuing event");
                self.queue_event(
                    did.clone(),
                    Queue::direct(peer_id, did.messaging(), payload_bytes.to_vec()),
                )
                .await;
            }
        }

        for recipient in recipient.iter().filter(|d| own_did.ne(d)) {
            if let Err(e) = self.request_key(conversation_id, recipient).await {
                tracing::warn!("Failed to send exchange request to {recipient}: {e}");
            }
        }

        self.event
            .emit(RayGunEventKind::ConversationCreated { conversation_id })
            .await;

        Ok(Conversation::from(&conversation))
    }

    async fn get(&self, id: Uuid) -> Result<ConversationDocument, Error> {
        self.root.get_conversation_document(id).await
    }

    pub async fn get_keystore(&self, id: Uuid) -> Result<Keystore, Error> {
        if !self.contains(id).await {
            return Err(Error::InvalidConversation);
        }

        self.root.get_keystore(id).await
    }

    pub async fn delete(&mut self, id: Uuid) -> Result<ConversationDocument, Error> {
        let conversation = self.get(id).await?;
        let mut meta = self
            .conversation_task
            .remove(&id)
            .ok_or(Error::InvalidConversation)?;

        let (tx, rx) = oneshot::channel();
        let _ = meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::Delete { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)??;

        meta.command_tx.close_channel();
        meta.handle.abort();

        Ok(conversation)
    }

    pub async fn delete_community_task(&mut self, id: Uuid) -> Result<CommunityDocument, Error> {
        let community = self.get_community_document(id).await?;
        let mut meta = self
            .community_task
            .remove(&id)
            .ok_or(Error::InvalidCommunity)?;

        let (tx, rx) = oneshot::channel();
        let _ = meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::Delete { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)??;

        meta.command_tx.close_channel();
        meta.handle.abort();

        Ok(community)
    }

    pub async fn list(&self) -> Vec<ConversationDocument> {
        self.list_stream().await.collect::<Vec<_>>().await
    }

    pub async fn list_stream(&self) -> impl Stream<Item = ConversationDocument> + Unpin {
        self.root.list_conversation_document().await
    }

    pub async fn contains(&self, id: Uuid) -> bool {
        self.list_stream()
            .await
            .any(|conversation| async move { conversation.id() == id })
            .await
    }

    pub async fn set_document<B: BorrowMut<ConversationDocument>>(
        &mut self,
        mut document: B,
    ) -> Result<(), Error> {
        let document = document.borrow_mut();
        let keypair = self.root.keypair();
        if let Some(creator) = document.creator.as_ref() {
            let did = keypair.to_did()?;
            if creator.eq(&did) && matches!(document.conversation_type(), ConversationType::Group) {
                document.sign(keypair)?;
            }
        }

        document.verify()?;

        self.root.set_conversation_document(document).await?;
        self.identity.export_root_document().await?;
        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        id: Uuid,
    ) -> Result<tokio::sync::broadcast::Sender<MessageEventKind>, Error> {
        let meta = self
            .conversation_task
            .get_mut(&id)
            .ok_or(Error::InvalidConversation)?;

        let (tx, rx) = oneshot::channel();
        let _ = meta
            .command_tx
            .clone()
            .send(ConversationTaskCommand::EventHandler { response: tx })
            .await;
        let tx = rx.await.map_err(anyhow::Error::from)?;

        Ok(tx)
    }

    pub async fn subscribe_community(
        &mut self,
        id: Uuid,
    ) -> Result<tokio::sync::broadcast::Sender<MessageEventKind>, Error> {
        let meta = self
            .community_task
            .get_mut(&id)
            .ok_or(Error::InvalidCommunity)?;

        let (tx, rx) = oneshot::channel();
        let _ = meta
            .command_tx
            .clone()
            .send(CommunityTaskCommand::EventHandler { response: tx })
            .await;
        let tx = rx.await.map_err(anyhow::Error::from)?;

        Ok(tx)
    }

    async fn queue_event(&mut self, did: DID, queue: Queue) {
        self.queue.entry(did).or_default().push(queue);
        self.save_queue().await
    }

    async fn save_queue(&self) {
        let key = self.ipfs.messaging_queue();
        let current_cid = self
            .ipfs
            .repo()
            .data_store()
            .get(key.as_bytes())
            .await
            .unwrap_or_default()
            .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
            .and_then(|cid_str| cid_str.parse::<Cid>().ok());

        let cid = match self.ipfs.put_dag(&self.queue).pin(true).await {
            Ok(cid) => cid,
            Err(e) => {
                tracing::error!(error = %e, "unable to save queue");
                return;
            }
        };

        let cid_str = cid.to_string();

        if let Err(e) = self
            .ipfs
            .repo()
            .data_store()
            .put(key.as_bytes(), cid_str.as_bytes())
            .await
        {
            tracing::error!(error = %e, "unable to save queue");
            return;
        }

        tracing::info!("messaging queue saved");

        let old_cid = current_cid;

        if let Some(old_cid) = old_cid {
            if old_cid != cid && self.ipfs.is_pinned(old_cid).await.unwrap_or_default() {
                let _ = self.ipfs.remove_pin(old_cid).recursive().await;
            }
        }
    }

    async fn request_key(&mut self, conversation_id: Uuid, did: &DID) -> Result<(), Error> {
        let request = ConversationRequestResponse::Request {
            conversation_id,
            kind: ConversationRequestKind::Key,
        };

        let conversation = self.get(conversation_id).await?;

        if !conversation.recipients().contains(did) {
            //TODO: user is not a recipient of the conversation
            return Err(Error::PublicKeyInvalid);
        }

        let keypair = self.root.keypair();

        let payload = PayloadBuilder::new(keypair, request)
            .add_recipient(did)?
            .from_ipfs(&self.ipfs)
            .await?;

        let payload_bytes = payload.to_bytes()?;

        let topic = conversation.exchange_topic(did);

        let peers = self.ipfs.pubsub_peers(Some(topic.clone())).await?;
        let peer_id = did.to_peer_id()?;
        if !peers.contains(&peer_id)
            || (peers.contains(&peer_id)
                && self
                    .ipfs
                    .pubsub_publish(topic.clone(), payload_bytes.clone())
                    .await
                    .is_err())
        {
            tracing::warn!(%conversation_id, "Unable to publish to topic");
            self.queue_event(
                did.clone(),
                Queue::direct(peer_id, topic.clone(), payload_bytes.clone()),
            )
            .await;
        }

        // TODO: Store request locally and hold any messages and events until key is received from peer

        Ok(())
    }

    async fn request_community_key(&mut self, community_id: Uuid, did: &DID) -> Result<(), Error> {
        let request = ConversationRequestResponse::Request {
            conversation_id: community_id,
            kind: ConversationRequestKind::Key,
        };

        let community = self.get_community_document(community_id).await?;

        if !community.participants().contains(did) {
            //TODO: user is not a recipient of the conversation
            return Err(Error::PublicKeyInvalid);
        }

        let keypair = self.root.keypair();

        let payload = PayloadBuilder::new(keypair, request)
            .add_recipient(did)?
            .from_ipfs(&self.ipfs)
            .await?;

        let topic = community.exchange_topic(did);

        let peers = self.ipfs.pubsub_peers(Some(topic.clone())).await?;
        let peer_id = did.to_peer_id()?;
        if !peers.contains(&peer_id)
            || (peers.contains(&peer_id)
                && self
                    .ipfs
                    .pubsub_publish(topic.clone(), payload.to_bytes()?)
                    .await
                    .is_err())
        {
            tracing::warn!(%community_id, "Unable to publish to topic");
            self.queue_event(
                did.clone(),
                Queue::direct(peer_id, topic.clone(), payload.to_bytes()?),
            )
            .await;
        }

        // TODO: Store request locally and hold any messages and events until key is received from peer

        Ok(())
    }

    pub async fn delete_conversation(
        &mut self,
        conversation_id: Uuid,
        broadcast: bool,
    ) -> Result<(), Error> {
        let document_type = self.delete(conversation_id).await?;

        let own_did = &self.identity.did_key();

        if broadcast {
            let recipients = document_type.recipients();

            let mut can_broadcast = true;

            if matches!(document_type.conversation_type(), ConversationType::Group) {
                let creator = document_type
                    .creator
                    .as_ref()
                    .ok_or(Error::InvalidConversation)?;

                if creator.ne(own_did) {
                    can_broadcast = false;
                    let recipients = recipients
                        .iter()
                        .filter(|did| own_did.ne(did))
                        .filter(|did| creator.ne(did))
                        .cloned()
                        .collect::<Vec<_>>();
                    if let Err(e) = self
                        .leave_group_conversation(creator, &recipients, conversation_id)
                        .await
                    {
                        tracing::error!(%conversation_id, error = %e, "Error leaving conversation");
                    }
                }
            }

            if can_broadcast {
                let peer_id_list = recipients
                    .clone()
                    .iter()
                    .filter(|did| own_did.ne(did))
                    .map(|did| (did.clone(), did))
                    .filter_map(|(a, b)| b.to_peer_id().map(|pk| (a, pk)).ok())
                    .collect::<Vec<_>>();

                let event = ConversationEvents::DeleteConversation {
                    conversation_id: document_type.id(),
                };

                let payload = PayloadBuilder::new(self.root.keypair(), event)
                    .add_recipients(peer_id_list.iter().map(|(did, _)| did))?
                    .from_ipfs(&self.ipfs)
                    .await?;

                let payload_bytes = payload.to_bytes()?;

                let main_timer = Instant::now();
                for (recipient, peer_id) in peer_id_list {
                    let peers = self.ipfs.pubsub_peers(Some(recipient.messaging())).await?;
                    let timer = Instant::now();
                    let mut time = true;
                    if !peers.contains(&peer_id)
                        || (peers.contains(&peer_id)
                            && self
                                .ipfs
                                .pubsub_publish(recipient.messaging(), payload_bytes.clone())
                                .await
                                .is_err())
                    {
                        tracing::warn!(%conversation_id, "Unable to publish to topic. Queuing event");
                        //Note: If the error is related to peer not available then we should push this to queue but if
                        //      its due to the message limit being reached we should probably break up the message to fix into
                        //      "max_transmit_size" within rust-libp2p gossipsub
                        //      For now we will queue the message if we hit an error
                        self.queue_event(
                            recipient.clone(),
                            Queue::direct(peer_id, recipient.messaging(), payload_bytes.clone()),
                        )
                        .await;
                        time = false;
                    }

                    if time {
                        let end = timer.elapsed();
                        tracing::info!(%conversation_id, "Event sent to {recipient}");
                        tracing::trace!(%conversation_id, "Took {}ms to send event", end.as_millis());
                    }
                }
                let main_timer_end = main_timer.elapsed();
                tracing::trace!(%conversation_id,
                    "Completed processing within {}ms",
                    main_timer_end.as_millis()
                );
            }
        }

        let conversation_id = document_type.id();

        self.event
            .emit(RayGunEventKind::ConversationDeleted { conversation_id })
            .await;

        Ok(())
    }

    async fn leave_group_conversation(
        &mut self,
        creator: &DID,
        list: &[DID],
        conversation_id: Uuid,
    ) -> Result<(), Error> {
        let own_did = self.identity.did_key();

        let context = format!("exclude {}", own_did);
        let signature = sign_serde(self.root.keypair(), &context)?;
        let signature = bs58::encode(signature).into_string();

        let event = ConversationEvents::LeaveConversation {
            conversation_id,
            recipient: own_did,
            signature,
        };

        //We want to send the event to the recipients until the creator can remove them from the conversation directly

        for did in list.iter() {
            if let Err(e) = self
                .send_single_conversation_event(conversation_id, did, event.clone())
                .await
            {
                tracing::error!(%conversation_id, error = %e, "Error sending conversation event to {did}");
                continue;
            }
        }

        self.send_single_conversation_event(conversation_id, creator, event)
            .await
    }

    async fn send_single_conversation_event(
        &mut self,
        conversation_id: Uuid,
        did_key: &DID,
        event: ConversationEvents,
    ) -> Result<(), Error> {
        let keypair = self.root.keypair();

        let payload = PayloadBuilder::new(keypair, event)
            .add_recipient(did_key)?
            .from_ipfs(&self.ipfs)
            .await?;

        let payload_bytes = payload.to_bytes()?;

        let peer_id = did_key.to_peer_id()?;
        let peers = self.ipfs.pubsub_peers(Some(did_key.messaging())).await?;

        let mut time = true;
        let timer = Instant::now();
        if !peers.contains(&peer_id)
            || (peers.contains(&peer_id)
                && self
                    .ipfs
                    .pubsub_publish(did_key.messaging(), payload_bytes.clone())
                    .await
                    .is_err())
        {
            tracing::warn!(%conversation_id, "Unable to publish to topic. Queuing event");
            self.queue_event(
                did_key.clone(),
                Queue::direct(peer_id, did_key.messaging(), payload_bytes.clone()),
            )
            .await;
            time = false;
        }
        if time {
            let end = timer.elapsed();
            tracing::info!(%conversation_id, "Event sent to {did_key}");
            tracing::trace!(%conversation_id, "Took {}ms to send event", end.as_millis());
        }

        Ok(())
    }
}
impl ConversationInner {
    async fn create_community_task(&mut self, community_id: Uuid) -> Result<(), Error> {
        let (ctx, crx) = mpsc::channel(256);

        let task = community_task::CommunityTask::new(
            community_id,
            &self.ipfs,
            &self.root,
            &self.identity,
            &self.file,
            &self.discovery,
            crx,
            self.event.clone(),
        )
        .await?;

        let handle = async_rt::task::spawn_abortable(task.run());

        tracing::info!(%community_id, "started community");

        let inner_meta = CommunityInnerMeta {
            command_tx: ctx,
            handle,
        };

        self.community_task.insert(community_id, inner_meta);

        Ok(())
    }

    async fn get_community_document(&self, id: Uuid) -> Result<CommunityDocument, Error> {
        self.root.get_community_document(id).await
    }

    pub async fn set_community_document<B: BorrowMut<CommunityDocument>>(
        &mut self,
        mut document: B,
    ) -> Result<(), Error> {
        let document = document.borrow_mut();
        let keypair = self.root.keypair();

        let did = keypair.to_did()?;
        if document.owner.eq(&did) {
            document.sign(keypair)?;
        }

        document.verify()?;

        self.root.set_community_document(document).await?;
        self.identity.export_root_document().await?;
        Ok(())
    }
    pub async fn contains_community(&self, id: Uuid) -> bool {
        self.list_community_stream()
            .await
            .any(|community| async move { community.id() == id })
            .await
    }
    pub async fn list_community(&self) -> Vec<CommunityDocument> {
        self.list_community_stream().await.collect::<Vec<_>>().await
    }
    pub async fn list_community_stream(&self) -> impl Stream<Item = CommunityDocument> + Unpin {
        self.root.list_community_document().await
    }
}

impl ConversationInner {
    pub async fn create_community(&mut self, name: &str) -> Result<Community, Error> {
        let name = name.trim();
        if name.is_empty() || name.len() > 255 {
            return Err(Error::InvalidLength {
                context: "name".into(),
                current: name.len(),
                minimum: Some(1),
                maximum: Some(255),
            });
        }

        let community = CommunityDocument::new(self.root.keypair(), name.to_owned())?;

        let community_id = community.id;

        self.set_community_document(community).await?;

        self.create_community_task(community_id).await?;

        let community = self.get_community_document(community_id).await?;

        self.event
            .emit(RayGunEventKind::CommunityCreated { community_id })
            .await;

        Ok(Community::from(community))
    }
    pub async fn delete_community(&mut self, community_id: Uuid) -> Result<(), Error> {
        let document = self.get_community_document(community_id).await?;
        let own_did = &self.identity.did_key();
        if &document.owner != own_did {
            return Err(Error::Unauthorized);
        }
        let doc = self.delete_community_task(community_id).await?;

        let peer_id_list = doc
            .participants()
            .clone()
            .iter()
            .filter(|did| own_did.ne(did))
            .map(|did| (did.clone(), did))
            .filter_map(|(a, b)| b.to_peer_id().map(|pk| (a, pk)).ok())
            .collect::<Vec<_>>();

        let event = ConversationEvents::DeleteCommunity {
            community_id: doc.id(),
        };

        let keypair = self.root.keypair();
        let payload = PayloadBuilder::new(keypair, event)
            .add_recipients(peer_id_list.iter().map(|(did, _)| did))?
            .from_ipfs(&self.ipfs)
            .await?;

        let main_timer = Instant::now();
        for (recipient, peer_id) in peer_id_list {
            let peers = self.ipfs.pubsub_peers(Some(recipient.messaging())).await?;
            let timer = Instant::now();
            let mut time = true;
            if !peers.contains(&peer_id)
                || (peers.contains(&peer_id)
                    && self
                        .ipfs
                        .pubsub_publish(recipient.messaging(), payload.to_bytes()?)
                        .await
                        .is_err())
            {
                tracing::warn!(%community_id, "Unable to publish to topic. Queuing event");
                //Note: If the error is related to peer not available then we should push this to queue but if
                //      its due to the message limit being reached we should probably break up the message to fix into
                //      "max_transmit_size" within rust-libp2p gossipsub
                //      For now we will queue the message if we hit an error
                self.queue_event(
                    recipient.clone(),
                    Queue::direct(peer_id, recipient.messaging(), payload.to_bytes()?),
                )
                .await;
                time = false;
            }

            if time {
                let end = timer.elapsed();
                tracing::info!(%community_id, "Event sent to {recipient}");
                tracing::trace!(%community_id, "Took {}ms to send event", end.as_millis());
            }
        }
        let main_timer_end = main_timer.elapsed();
        tracing::trace!(%community_id,
            "Completed processing within {}ms",
            main_timer_end.as_millis()
        );

        let community_id = doc.id();
        self.event
            .emit(RayGunEventKind::CommunityDeleted { community_id })
            .await;
        Ok(())
    }
    pub async fn get_community(&mut self, community_id: Uuid) -> Result<Community, Error> {
        let doc = self.get_community_document(community_id).await?;
        let own_did = &self.identity.did_key();
        if !doc.participants().contains(own_did) {
            return Err(Error::Unauthorized);
        }
        Ok(doc.into())
    }

    pub async fn list_communities_joined(&self) -> Result<IndexSet<Uuid>, Error> {
        let own_did = &self.identity.did_key();
        Ok(self
            .list_community()
            .await
            .iter()
            .filter_map(|c| {
                if &c.owner == own_did || c.members.contains(own_did) {
                    Some(c.id)
                } else {
                    None
                }
            })
            .collect())
    }
}

async fn process_conversation(
    this: &mut ConversationInner,
    sender: PeerId,
    event: ConversationEvents,
) -> Result<(), Error> {
    match event {
        ConversationEvents::NewConversation { recipient } => {
            let keypair = this.root.keypair();
            let did = this.identity.did_key();
            tracing::info!("New conversation event received from {recipient}");
            let conversation_id =
                generate_shared_topic(keypair, &recipient, Some("direct-conversation"))?;

            if this.contains(conversation_id).await {
                tracing::warn!(%conversation_id, "Conversation exist");
                return Ok(());
            }

            let is_blocked = this.root.is_blocked(&recipient).await?;

            if is_blocked {
                //TODO: Signal back to close conversation
                tracing::warn!("{recipient} is blocked");
                return Err(Error::PublicKeyIsBlocked);
            }

            let list = [did.clone(), recipient];
            tracing::info!(%conversation_id, "Creating conversation");

            let convo = ConversationDocument::new_direct(keypair, list)?;
            let conversation_type = convo.conversation_type();

            this.set_document(convo).await?;

            tracing::info!(%conversation_id, %conversation_type, "conversation created");

            this.create_conversation_task(conversation_id).await?;

            this.event
                .emit(RayGunEventKind::ConversationCreated { conversation_id })
                .await;
        }
        ConversationEvents::NewGroupConversation { mut conversation } => {
            let did = this.identity.did_key();

            let conversation_id = conversation.id;
            tracing::info!(%conversation_id, "New group conversation event received");

            if this.contains(conversation_id).await {
                tracing::warn!(%conversation_id, "Conversation exist");
                return Ok(());
            }

            if !conversation.recipients.contains(&did) {
                tracing::warn!(%conversation_id, "was added to conversation but never was apart of the conversation.");
                return Ok(());
            }

            tracing::info!(%conversation_id, "Creating group conversation");

            let conversation_type = conversation.conversation_type();

            conversation.verify()?;

            //TODO: Resolve message list
            conversation.messages = None;
            conversation.archived = false;
            conversation.favorite = false;

            this.set_document(conversation).await?;

            this.create_conversation_task(conversation_id).await?;

            let conversation = this.get(conversation_id).await?;

            tracing::info!(%conversation_id, "{} conversation created", conversation_type);

            for recipient in conversation.recipients.iter().filter(|d| did.ne(d)) {
                if let Err(e) = this.request_key(conversation_id, recipient).await {
                    tracing::warn!(%conversation_id, error = %e, %recipient, "Failed to send exchange request");
                }
            }

            this.event
                .emit(RayGunEventKind::ConversationCreated { conversation_id })
                .await;
        }
        ConversationEvents::LeaveConversation {
            conversation_id,
            recipient,
            signature,
        } => {
            let conversation_meta = this
                .conversation_task
                .get(&conversation_id)
                .ok_or(Error::InvalidConversation)?;
            let (tx, rx) = oneshot::channel();
            let _ = conversation_meta
                .command_tx
                .clone()
                .send(ConversationTaskCommand::AddExclusion {
                    member: recipient,
                    signature,
                    response: tx,
                })
                .await;
            rx.await.map_err(anyhow::Error::from)??;
        }
        ConversationEvents::DeleteConversation { conversation_id } => {
            tracing::trace!("Delete conversation event received for {conversation_id}");
            if !this.contains(conversation_id).await {
                return Err(anyhow::anyhow!("Conversation {conversation_id} doesnt exist").into());
            }

            let sender = sender.to_did()?;

            match this.get(conversation_id).await {
                Ok(conversation)
                    if conversation.recipients().contains(&sender)
                        && matches!(conversation.conversation_type(), ConversationType::Direct)
                        || matches!(conversation.conversation_type(), ConversationType::Group)
                            && matches!(&conversation.creator, Some(creator) if creator.eq(&sender)) =>
                {
                    conversation
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Conversation exist but did not match condition required"
                    )
                    .into());
                }
            };

            this.delete_conversation(conversation_id, false).await?;
        }
        ConversationEvents::NewCommunityInvite {
            community_id,
            invite,
        } => {
            let mut updated = false;
            for i in this.community_invites.len()..0 {
                let (community, invitation) = this.community_invites[i].clone();
                if community == community_id && invitation.id == invite.id {
                    this.community_invites[i] = (community_id, invite.clone());
                    updated = true;
                    break;
                }
            }
            if !updated {
                this.community_invites.push((community_id, invite.clone()));
            }

            this.event
                .emit(RayGunEventKind::CommunityInvited {
                    community_id,
                    invite_id: invite.id,
                })
                .await;
        }
        ConversationEvents::DeleteCommunityInvite {
            community_id,
            invite,
        } => {
            for i in this.community_invites.len()..0 {
                let (community, invitation) = this.community_invites[i].clone();
                if community == community_id && invitation.id == invite.id {
                    this.community_invites.swap_remove(i);
                    this.event
                        .emit(RayGunEventKind::CommunityUninvited {
                            community_id,
                            invite_id: invite.id,
                        })
                        .await;
                    break;
                }
            }
        }
        ConversationEvents::JoinCommunity {
            community_id,
            community_document: result,
        } => match result {
            None => {
                this.event
                    .emit(RayGunEventKind::CommunityJoinRejected { community_id })
                    .await;
                return Ok(());
            }
            Some(community_document) => {
                for i in this.community_invites.len()..0 {
                    let (community, _) = this.community_invites[i];
                    if community == community_id {
                        this.community_invites.swap_remove(i);
                    }
                }

                let did = this.identity.did_key();

                if this.contains_community(community_id).await {
                    return Ok(());
                }

                let recipients = community_document.participants().clone();

                for recipient in &recipients {
                    if !this.discovery.contains(recipient).await {
                        let _ = this.discovery.insert(recipient).await;
                    }
                }

                this.set_community_document(community_document).await?;

                this.create_community_task(community_id).await?;

                for recipient in recipients.iter().filter(|d| did.ne(d)) {
                    if let Err(e) = this.request_community_key(community_id, recipient).await {
                        tracing::warn!(%community_id, error = %e, %recipient, "Failed to send exchange request");
                    }
                }

                let community_meta = this
                    .community_task
                    .get(&community_id)
                    .ok_or(Error::InvalidCommunity)?;
                let (tx, rx) = oneshot::channel();
                let _ = community_meta
                    .command_tx
                    .clone()
                    .send(CommunityTaskCommand::SendJoinedCommunityEvent { response: tx })
                    .await;
                let _ = rx.await.map_err(anyhow::Error::from)?;

                this.event
                    .emit(RayGunEventKind::CommunityJoined { community_id })
                    .await;
            }
        },
        ConversationEvents::DeleteCommunity { community_id } => {
            tracing::trace!("Delete community event received for {community_id}");
            if !this.contains_community(community_id).await {
                return Err(anyhow::anyhow!("Community {community_id} doesnt exist").into());
            }

            let sender = sender.to_did()?;

            match this.get_community_document(community_id).await {
                Ok(community) if community.owner.eq(&sender) => community,
                _ => {
                    return Err(anyhow::anyhow!(
                        "Community exist but did not match condition required"
                    )
                    .into());
                }
            };

            let doc = this.delete_community_task(community_id).await?;
            let community_id = doc.id();
            this.event
                .emit(RayGunEventKind::CommunityDeleted { community_id })
                .await;
        }
    }
    Ok(())
}

async fn process_identity_events(
    this: &mut ConversationInner,
    event: MultiPassEventKind,
) -> Result<(), Error> {
    //TODO: Tie this into a configuration
    let with_friends = false;

    let own_did = this.identity.did_key();

    match event {
        MultiPassEventKind::FriendAdded { did } => {
            if !with_friends {
                return Ok(());
            }

            match this.create_conversation(&did).await {
                Ok(_) | Err(Error::ConversationExist { .. }) => return Ok(()),
                Err(e) => return Err(e),
            }
        }

        MultiPassEventKind::Blocked { did } | MultiPassEventKind::BlockedBy { did } => {
            let list = this.list().await;

            for conversation in list.iter().filter(|c| c.recipients().contains(&did)) {
                let id = conversation.id();
                match conversation.conversation_type() {
                    ConversationType::Direct => {
                        if let Err(e) = this.delete_conversation(id, true).await {
                            tracing::warn!(conversation_id = %id, error = %e, "Failed to delete conversation");
                            continue;
                        }
                    }
                    ConversationType::Group => {
                        if conversation.creator != Some(own_did.clone()) {
                            continue;
                        }

                        let conversation_meta = this
                            .conversation_task
                            .get(&id)
                            .ok_or(Error::InvalidConversation)?;

                        let (tx, rx) = oneshot::channel();
                        let _ = conversation_meta
                            .command_tx
                            .clone()
                            .send(ConversationTaskCommand::RemoveParticipant {
                                member: did.clone(),
                                broadcast: true,
                                response: tx,
                            })
                            .await;

                        let Ok(result) = rx.await else {
                            continue;
                        };

                        if let Err(e) = result {
                            tracing::warn!(conversation_id = %id, error = %e, "Failed to remove {did} from conversation");
                            continue;
                        }

                        if this.root.is_blocked(&did).await.unwrap_or_default() {
                            let (tx, rx) = oneshot::channel();
                            let _ = conversation_meta
                                .command_tx
                                .clone()
                                .send(ConversationTaskCommand::AddRestricted {
                                    member: did.clone(),
                                    response: tx,
                                })
                                .await;
                            let _ = rx.await;
                        }
                    }
                }
            }
        }
        MultiPassEventKind::Unblocked { did } => {
            let list = this.list().await;

            for conversation in list
                .iter()
                .filter(|c| {
                    c.creator
                        .as_ref()
                        .map(|creator| own_did.eq(creator))
                        .unwrap_or_default()
                })
                .filter(|c| c.conversation_type() == ConversationType::Group)
                .filter(|c| c.restrict.contains(&did))
            {
                let id = conversation.id();

                let conversation_meta = this
                    .conversation_task
                    .get(&id)
                    .ok_or(Error::InvalidConversation)?;

                let (tx, rx) = oneshot::channel();
                let _ = conversation_meta
                    .command_tx
                    .clone()
                    .send(ConversationTaskCommand::AddRestricted {
                        member: did.clone(),
                        response: tx,
                    })
                    .await;
                let _ = rx.await;
            }
        }
        MultiPassEventKind::FriendRemoved { did } => {
            if !with_friends {
                return Ok(());
            }

            let list = this.list().await;

            for conversation in list.iter().filter(|c| c.recipients().contains(&did)) {
                let id = conversation.id();
                match conversation.conversation_type() {
                    ConversationType::Direct => {
                        if let Err(e) = this.delete_conversation(id, true).await {
                            tracing::warn!(conversation_id = %id, error = %e, "Failed to delete conversation");
                            continue;
                        }
                    }
                    ConversationType::Group => {
                        if conversation.creator != Some(own_did.clone()) {
                            continue;
                        }

                        let conversation_meta = this
                            .conversation_task
                            .get(&id)
                            .ok_or(Error::InvalidConversation)?;

                        let (tx, rx) = oneshot::channel();
                        let _ = conversation_meta
                            .command_tx
                            .clone()
                            .send(ConversationTaskCommand::RemoveParticipant {
                                member: did.clone(),
                                broadcast: true,
                                response: tx,
                            })
                            .await;

                        let Ok(result) = rx.await else {
                            continue;
                        };

                        if let Err(e) = result {
                            tracing::warn!(conversation_id = %id, error = %e, "Failed to remove {did} from conversation");
                            continue;
                        }
                    }
                }
            }
        }
        MultiPassEventKind::IdentityOnline { .. } => {
            //TODO: Check queue and process any entry once peer is subscribed to the respective topics.
        }
        _ => {}
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
struct Queue {
    peer: PeerId,
    topic: String,
    data: Bytes,
    sent: bool,
}

impl Queue {
    pub fn direct(peer: PeerId, topic: String, data: impl Into<Bytes>) -> Self {
        let data = data.into();
        Queue {
            peer,
            topic,
            data,
            sent: false,
        }
    }
}

//TODO: Replace
async fn _process_queue(this: &mut ConversationInner) {
    let mut changed = false;
    for (did, items) in this.queue.iter_mut() {
        let Ok(peer_id) = did.to_peer_id() else {
            continue;
        };

        if !this.ipfs.is_connected(peer_id).await.unwrap_or_default() {
            continue;
        }

        for item in items {
            let Queue {
                peer,
                topic,
                data,
                sent,
                ..
            } = item;

            if !this
                .ipfs
                .pubsub_peers(Some(topic.clone()))
                .await
                .map(|list| list.contains(peer))
                .unwrap_or_default()
            {
                continue;
            }

            if *sent {
                continue;
            }

            if let Err(e) = this.ipfs.pubsub_publish(topic.clone(), data.clone()).await {
                tracing::error!("Error publishing to topic: {e}");
                continue;
            }

            *sent = true;

            changed = true;
        }
    }

    this.queue.retain(|_, queue| {
        queue.retain(|item| !item.sent);
        !queue.is_empty()
    });

    if changed {
        this.save_queue().await;
    }
}
