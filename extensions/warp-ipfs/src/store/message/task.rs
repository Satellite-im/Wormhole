use bytes::Bytes;
use chrono::Utc;
use either::Either;
use futures::channel::oneshot;
use futures::stream::{self, BoxStream, FuturesUnordered};
use futures::{FutureExt, SinkExt, StreamExt, TryFutureExt};
use futures_timeout::TimeoutExt;
use futures_timer::Delay;
use indexmap::{IndexMap, IndexSet};
use ipld_core::cid::Cid;
use pollable_map::futures::FutureMap;
use rust_ipfs::p2p::MultiaddrExt;
use rust_ipfs::{libp2p::gossipsub::Message, Ipfs};
use rust_ipfs::{IpfsPath, PeerId, SubscriptionStream};
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tokio_stream::StreamMap;
use uuid::Uuid;
use warp::constellation::directory::Directory;
use warp::constellation::{ConstellationProgressStream, Progression};
use warp::crypto::DID;
use warp::raygun::{
    AttachmentEventStream, AttachmentKind, ConversationImage, GroupPermissionOpt, Location,
    LocationKind, MessageEvent, MessageOptions, MessageReference, MessageStatus, MessageType,
    Messages, MessagesType, RayGunEventKind,
};
use warp::{
    crypto::{cipher::Cipher, generate},
    error::Error,
    raygun::{
        ConversationType, GroupPermission, ImplGroupPermissions, MessageEventKind, PinState,
        ReactionState,
    },
};
use web_time::Instant;

use crate::config;
use crate::shuttle::message::client::MessageCommand;
use crate::store::conversation::message::MessageDocument;
use crate::store::discovery::Discovery;
use crate::store::document::files::FileDocument;
use crate::store::document::image_dag::ImageDag;
use crate::store::ds_key::DataStoreKey;
use crate::store::event_subscription::EventSubscription;
use crate::store::message::CHAT_DIRECTORY;
use crate::store::topics::PeerTopic;
use crate::store::{
    ecdh_shared_key, verify_serde_sig, ConversationEvents, ConversationImageType,
    MAX_CONVERSATION_BANNER_SIZE, MAX_CONVERSATION_ICON_SIZE, SHUTTLE_TIMEOUT,
};
use crate::utils::{ByteCollection, ExtensionType};
use crate::{
    // rt::LocalExecutor,
    store::{
        conversation::ConversationDocument,
        document::root::RootDocumentMap,
        ecdh_decrypt, ecdh_encrypt,
        files::FileStore,
        identity::IdentityStore,
        keystore::Keystore,
        payload::{PayloadBuilder, PayloadMessage},
        ConversationRequestKind, ConversationRequestResponse, ConversationResponseKind,
        ConversationUpdateKind, DidExt, MessagingEvents, PeerIdExt, MAX_CONVERSATION_DESCRIPTION,
        MAX_MESSAGE_SIZE, MAX_REACTIONS, MIN_MESSAGE_SIZE,
    },
};

type AttachmentOneshot = (MessageDocument, oneshot::Sender<Result<(), Error>>);

use super::DownloadStream;

#[allow(dead_code)]
pub enum ConversationTaskCommand {
    SetDescription {
        desc: Option<String>,
        response: oneshot::Sender<Result<(), Error>>,
    },
    FavoriteConversation {
        favorite: bool,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetMessage {
        message_id: Uuid,
        response: oneshot::Sender<Result<warp::raygun::Message, Error>>,
    },
    GetMessages {
        options: MessageOptions,
        response: oneshot::Sender<Result<Messages, Error>>,
    },
    GetMessagesCount {
        response: oneshot::Sender<Result<usize, Error>>,
    },
    GetMessageReference {
        message_id: Uuid,
        response: oneshot::Sender<Result<MessageReference, Error>>,
    },
    GetMessageReferences {
        options: MessageOptions,
        response: oneshot::Sender<Result<BoxStream<'static, MessageReference>, Error>>,
    },
    UpdateConversationName {
        name: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    UpdateConversationPermissions {
        permissions: GroupPermissionOpt,
        response: oneshot::Sender<Result<(), Error>>,
    },
    AddParticipant {
        member: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RemoveParticipant {
        member: DID,
        broadcast: bool,
        response: oneshot::Sender<Result<(), Error>>,
    },
    MessageStatus {
        message_id: Uuid,
        response: oneshot::Sender<Result<MessageStatus, Error>>,
    },

    SendMessage {
        lines: Vec<String>,
        response: oneshot::Sender<Result<Uuid, Error>>,
    },
    EditMessage {
        message_id: Uuid,
        lines: Vec<String>,
        response: oneshot::Sender<Result<(), Error>>,
    },
    ReplyMessage {
        message_id: Uuid,
        lines: Vec<String>,
        response: oneshot::Sender<Result<Uuid, Error>>,
    },
    DeleteMessage {
        message_id: Uuid,
        response: oneshot::Sender<Result<(), Error>>,
    },
    PinMessage {
        message_id: Uuid,
        state: PinState,
        response: oneshot::Sender<Result<(), Error>>,
    },
    ReactMessage {
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    AttachMessage {
        message_id: Option<Uuid>,
        locations: Vec<Location>,
        lines: Vec<String>,
        response: oneshot::Sender<Result<(Uuid, AttachmentEventStream), Error>>,
    },
    DownloadAttachment {
        message_id: Uuid,
        file: String,
        path: PathBuf,
        response: oneshot::Sender<Result<ConstellationProgressStream, Error>>,
    },
    DownloadAttachmentStream {
        message_id: Uuid,
        file: String,
        response: oneshot::Sender<Result<DownloadStream, Error>>,
    },
    SendEvent {
        event: MessageEvent,
        response: oneshot::Sender<Result<(), Error>>,
    },
    CancelEvent {
        event: MessageEvent,
        response: oneshot::Sender<Result<(), Error>>,
    },
    UpdateIcon {
        location: Location,
        response: oneshot::Sender<Result<(), Error>>,
    },
    UpdateBanner {
        location: Location,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RemoveIcon {
        response: oneshot::Sender<Result<(), Error>>,
    },
    RemoveBanner {
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetIcon {
        response: oneshot::Sender<Result<ConversationImage, Error>>,
    },
    GetBanner {
        response: oneshot::Sender<Result<ConversationImage, Error>>,
    },
    ArchivedConversation {
        response: oneshot::Sender<Result<(), Error>>,
    },
    UnarchivedConversation {
        response: oneshot::Sender<Result<(), Error>>,
    },

    AddExclusion {
        member: DID,
        signature: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    AddRestricted {
        member: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RemoveRestricted {
        member: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },

    EventHandler {
        response: oneshot::Sender<tokio::sync::broadcast::Sender<MessageEventKind>>,
    },
}

pub struct ConversationTask {
    conversation_id: Uuid,
    ipfs: Ipfs,
    root: RootDocumentMap,
    file: FileStore,
    identity: IdentityStore,
    discovery: Discovery,
    pending_key_exchange: IndexMap<DID, (Vec<u8>, bool)>,
    pending_key_request_sent: IndexSet<DID>,
    document: ConversationDocument,
    keystore: Keystore,

    messaging_stream: SubscriptionStream,
    event_stream: SubscriptionStream,
    request_stream: SubscriptionStream,

    attachment_tx: futures::channel::mpsc::Sender<AttachmentOneshot>,
    attachment_rx: futures::channel::mpsc::Receiver<AttachmentOneshot>,
    message_command: futures::channel::mpsc::Sender<MessageCommand>,
    event_broadcast: tokio::sync::broadcast::Sender<MessageEventKind>,
    event_subscription: EventSubscription<RayGunEventKind>,

    pending_ping_response: FutureMap<DID, Delay>,
    ping_duration: IndexMap<DID, Instant>,
    command_rx: futures::channel::mpsc::Receiver<ConversationTaskCommand>,

    //TODO: replace queue
    queue: HashMap<DID, Vec<QueueItem>>,
}

impl ConversationTask {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        conversation_id: Uuid,
        ipfs: &Ipfs,
        root: &RootDocumentMap,
        identity: &IdentityStore,
        file: &FileStore,
        discovery: &Discovery,
        command_rx: futures::channel::mpsc::Receiver<ConversationTaskCommand>,
        message_command: futures::channel::mpsc::Sender<MessageCommand>,
        event_subscription: EventSubscription<RayGunEventKind>,
    ) -> Result<Self, Error> {
        let document = root.get_conversation_document(conversation_id).await?;
        let main_topic = document.topic();
        let event_topic = document.event_topic();
        let request_topic = document.exchange_topic(&identity.did_key());

        let messaging_stream = ipfs.pubsub_subscribe(main_topic).await?;

        let event_stream = ipfs.pubsub_subscribe(event_topic).await?;

        let request_stream = ipfs.pubsub_subscribe(request_topic).await?;

        let (atx, arx) = futures::channel::mpsc::channel(256);
        let (btx, _) = tokio::sync::broadcast::channel(1024);
        let mut task = Self {
            conversation_id,
            ipfs: ipfs.clone(),
            root: root.clone(),
            file: file.clone(),
            identity: identity.clone(),
            discovery: discovery.clone(),
            pending_key_exchange: Default::default(),
            pending_key_request_sent: Default::default(),
            document,
            keystore: Keystore::default(),

            messaging_stream,
            request_stream,
            event_stream,

            attachment_tx: atx,
            attachment_rx: arx,
            event_broadcast: btx,
            pending_ping_response: FutureMap::default(),
            ping_duration: IndexMap::new(),
            event_subscription,
            message_command,
            command_rx,
            queue: Default::default(),
        };

        task.keystore = match task.document.conversation_type() {
            ConversationType::Direct => Keystore::new(),
            ConversationType::Group => {
                match root.get_conversation_keystore(conversation_id).await {
                    Ok(store) => store,
                    Err(_) => {
                        let mut store = Keystore::new();
                        store.insert(
                            root.keypair(),
                            &identity.did_key(),
                            warp::crypto::generate::<64>(),
                        )?;
                        task.set_keystore().await?;
                        store
                    }
                }
            }
        };

        let key = format!("{}/{}", ipfs.messaging_queue(), conversation_id);

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
            task.queue = data;
        }

        tracing::info!(%conversation_id, "conversation task created");
        Ok(task)
    }
}

impl ConversationTask {
    pub async fn run(mut self) {
        let this = &mut self;

        let conversation_id = this.conversation_id;

        let mut queue_timer = Delay::new(Duration::from_secs(1));

        let mut pending_exchange_timer = Delay::new(Duration::from_secs(1));

        let mut check_mailbox = Delay::new(Duration::from_secs(5));

        let mut ping_timer = Delay::new(Duration::from_secs(1));

        loop {
            tokio::select! {
                biased;
                Some(command) = this.command_rx.next() => {
                    this.process_command(command).await;
                }
                Some((message, response)) = this.attachment_rx.next() => {
                    let _ = response.send(this.store_direct_for_attachment(message).await);
                }
                Some((_id, _)) = this.pending_ping_response.next() => {
                    //TODO: score against identity that didnt respond in time
                }
                Some(request) = this.request_stream.next() => {
                    let source = request.source;
                    if let Err(e) = process_request_response_event(this, request).await {
                        tracing::error!(%conversation_id, sender = ?source, error = %e, name = "request", "Failed to process payload");
                    }
                }
                Some(event) = this.event_stream.next() => {
                    let source = event.source;
                    if let Err(e) = process_conversation_event(this, event).await {
                        tracing::error!(%conversation_id, sender = ?source, error = %e, name = "ev", "Failed to process payload");
                    }
                }
                Some(message) = this.messaging_stream.next() => {
                    let source = message.source;
                    if let Err(e) = this.process_msg_event(message).await {
                        tracing::error!(%conversation_id, sender = ?source, error = %e, name = "msg", "Failed to process payload");
                    }
                },
                _ = &mut queue_timer => {
                    _ = process_queue(this).await;
                    queue_timer.reset(Duration::from_secs(1));
                }
                _ = &mut pending_exchange_timer => {
                    _ = process_pending_payload(this).await;
                    pending_exchange_timer.reset(Duration::from_secs(1));
                }
                _ = &mut check_mailbox => {
                    _ = this.load_from_mailbox().await;
                    check_mailbox.reset(Duration::from_secs(60));
                }
                _ = &mut ping_timer => {
                    _ = this.ping_all().await;
                    ping_timer.reset(Duration::from_secs(60));
                }
            }
        }
    }
}

impl ConversationTask {
    async fn load_from_mailbox(&mut self) -> Result<(), Error> {
        let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config().clone()
        else {
            return Ok(());
        };

        let ipfs = self.ipfs.clone();
        let message_command = self.message_command.clone();
        let addresses = addresses.clone();
        let conversation_id = self.conversation_id;

        let mut mailbox = BTreeMap::new();
        let mut providers = vec![];
        for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
            let (tx, rx) = futures::channel::oneshot::channel();
            let _ = message_command
                .clone()
                .send(MessageCommand::FetchMailbox {
                    peer_id,
                    conversation_id,
                    response: tx,
                })
                .await;

            match rx.timeout(SHUTTLE_TIMEOUT).await {
                Ok(Ok(Ok(list))) => {
                    providers.push(peer_id);
                    mailbox.extend(list);
                    break;
                }
                Ok(Ok(Err(e))) => {
                    tracing::error!("unable to get mailbox to conversation {conversation_id} from {peer_id}: {e}");
                    break;
                }
                Ok(Err(_)) => {
                    tracing::error!("Channel been unexpectedly closed for {peer_id}");
                    continue;
                }
                Err(_) => {
                    tracing::error!("Request timed out for {peer_id}");
                    continue;
                }
            }
        }

        let conversation_mailbox = mailbox
            .into_iter()
            .filter_map(|(id, cid)| {
                let id = Uuid::from_str(&id).ok()?;
                Some((id, cid))
            })
            .collect::<BTreeMap<Uuid, Cid>>();

        let mut messages =
            FuturesUnordered::from_iter(conversation_mailbox.into_iter().map(|(id, cid)| {
                let ipfs = ipfs.clone();
                async move {
                    ipfs.fetch(&cid).recursive().await?;
                    Ok((id, cid))
                }
                .boxed()
            }))
            .filter_map(|res: Result<_, anyhow::Error>| async move { res.ok() })
            .filter_map(|(_, cid)| {
                let ipfs = ipfs.clone();
                let providers = providers.clone();
                let addresses = addresses.clone();
                let message_command = message_command.clone();
                async move {
                    let message_document = ipfs
                        .get_dag(cid)
                        .providers(&providers)
                        .deserialized::<MessageDocument>()
                        .await
                        .ok()?;

                    if !message_document.verify() {
                        return None;
                    }

                    for peer_id in addresses.into_iter().filter_map(|addr| addr.peer_id()) {
                        let _ = message_command
                            .clone()
                            .send(MessageCommand::MessageDelivered {
                                peer_id,
                                conversation_id,
                                message_id: message_document.id,
                            })
                            .await;
                    }
                    Some(message_document)
                }
            })
            .collect::<Vec<_>>()
            .await;

        messages.sort_by(|a, b| b.cmp(a));

        for message in messages {
            if !message.verify() {
                continue;
            }
            let message_id = message.id;
            match self
                .document
                .contains(&self.ipfs, message_id)
                .await
                .unwrap_or_default()
            {
                true => {
                    let current_message = self
                        .document
                        .get_message_document(&self.ipfs, message_id)
                        .await?;

                    self.document
                        .update_message_document(&self.ipfs, &message)
                        .await?;

                    let is_edited = matches!((message.modified, current_message.modified), (Some(modified), Some(current_modified)) if modified > current_modified )
                        | matches!(
                            (message.modified, current_message.modified),
                            (Some(_), None)
                        );

                    match is_edited {
                        true => {
                            let _ = self.event_broadcast.send(MessageEventKind::MessageEdited {
                                conversation_id,
                                message_id,
                            });
                        }
                        false => {
                            //TODO: Emit event showing message was updated in some way
                        }
                    }
                }
                false => {
                    self.document
                        .insert_message_document(&self.ipfs, &message)
                        .await?;

                    let _ = self
                        .event_broadcast
                        .send(MessageEventKind::MessageReceived {
                            conversation_id,
                            message_id,
                        });
                }
            }
        }

        self.set_document().await?;

        Ok(())
    }

    async fn process_command(&mut self, command: ConversationTaskCommand) {
        match command {
            ConversationTaskCommand::SetDescription { desc, response } => {
                let result = self.set_description(desc.as_deref()).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::FavoriteConversation { favorite, response } => {
                let result = self.set_favorite_conversation(favorite).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::GetMessage {
                message_id,
                response,
            } => {
                let result = self.get_message(message_id).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::GetMessages { options, response } => {
                let result = self.get_messages(options).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::GetMessagesCount { response } => {
                let result = self.messages_count().await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::GetMessageReference {
                message_id,
                response,
            } => {
                let result = self.get_message_reference(message_id).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::GetMessageReferences { options, response } => {
                let result = self.get_message_references(options).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::UpdateConversationName { name, response } => {
                let result = self.update_conversation_name(&name).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::UpdateConversationPermissions {
                permissions,
                response,
            } => {
                let result = self.update_conversation_permissions(permissions).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::AddParticipant { member, response } => {
                let result = self.add_participant(&member).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::RemoveParticipant {
                member,
                broadcast,
                response,
            } => {
                let result = self.remove_participant(&member, broadcast).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::MessageStatus {
                message_id,
                response,
            } => {
                let result = self.message_status(message_id).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::SendMessage { lines, response } => {
                let result = self.send_message(lines).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::EditMessage {
                message_id,
                lines,
                response,
            } => {
                let result = self.edit_message(message_id, lines).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::ReplyMessage {
                message_id,
                lines,
                response,
            } => {
                let result = self.reply_message(message_id, lines).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::DeleteMessage {
                message_id,
                response,
            } => {
                let result = self.delete_message(message_id, true).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::PinMessage {
                message_id,
                state,
                response,
            } => {
                let result = self.pin_message(message_id, state).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::ReactMessage {
                message_id,
                state,
                emoji,
                response,
            } => {
                let result = self.react(message_id, state, emoji).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::AttachMessage {
                message_id,
                locations,
                lines,
                response,
            } => {
                let result = self.attach(message_id, locations, lines);
                let _ = response.send(result);
            }
            ConversationTaskCommand::DownloadAttachment {
                message_id,
                file,
                path,
                response,
            } => {
                let result = self.download(message_id, &file, path).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::DownloadAttachmentStream {
                message_id,
                file,
                response,
            } => {
                let result = self.download_stream(message_id, &file).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::SendEvent { event, response } => {
                let result = self.send_event(event).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::CancelEvent { event, response } => {
                let result = self.cancel_event(event).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::UpdateIcon { location, response } => {
                let result = self
                    .update_conversation_image(location, ConversationImageType::Icon)
                    .await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::UpdateBanner { location, response } => {
                let result = self
                    .update_conversation_image(location, ConversationImageType::Banner)
                    .await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::RemoveIcon { response } => {
                let result = self
                    .remove_conversation_image(ConversationImageType::Icon)
                    .await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::RemoveBanner { response } => {
                let result = self
                    .remove_conversation_image(ConversationImageType::Banner)
                    .await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::GetIcon { response } => {
                let result = self.conversation_image(ConversationImageType::Icon).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::GetBanner { response } => {
                let result = self.conversation_image(ConversationImageType::Banner).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::ArchivedConversation { response } => {
                let result = self.archived_conversation().await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::UnarchivedConversation { response } => {
                let result = self.unarchived_conversation().await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::AddExclusion {
                member,
                signature,
                response,
            } => {
                let result = self.add_exclusion(member, signature).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::AddRestricted { member, response } => {
                let result = self.add_restricted(&member).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::RemoveRestricted { member, response } => {
                let result = self.remove_restricted(&member).await;
                let _ = response.send(result);
            }
            ConversationTaskCommand::EventHandler { response } => {
                let sender = self.event_broadcast.clone();
                let _ = response.send(sender);
            }
        }
    }
}

impl ConversationTask {
    pub async fn set_keystore(&mut self) -> Result<(), Error> {
        let mut map = self.root.get_conversation_keystore_map().await?;

        let id = self.conversation_id.to_string();
        let cid = self.ipfs.put_dag(&self.keystore).await?;

        map.insert(id, cid);

        self.root.set_conversation_keystore_map(map).await
    }

    pub async fn set_document(&mut self) -> Result<(), Error> {
        let keypair = self.root.keypair();
        if let Some(creator) = self.document.creator.as_ref() {
            let did = keypair.to_did()?;
            if creator.eq(&did)
                && matches!(self.document.conversation_type(), ConversationType::Group)
            {
                self.document.sign(keypair)?;
            }
        }

        self.document.verify()?;

        self.root.set_conversation_document(&self.document).await?;
        self.identity.export_root_document().await?;
        Ok(())
    }

    pub async fn replace_document(
        &mut self,
        mut document: ConversationDocument,
    ) -> Result<(), Error> {
        let keypair = self.root.keypair();
        if let Some(creator) = document.creator.as_ref() {
            let did = keypair.to_did()?;
            if creator.eq(&did) && matches!(document.conversation_type(), ConversationType::Group) {
                document.sign(keypair)?;
            }
        }

        document.verify()?;

        self.root.set_conversation_document(&document).await?;
        self.identity.export_root_document().await?;
        self.document = document;
        Ok(())
    }

    async fn ping(&mut self, identity: &DID) -> Result<(), Error> {
        let keypair = self.root.keypair();
        let request = ConversationRequestResponse::Request {
            conversation_id: self.conversation_id,
            kind: ConversationRequestKind::Ping,
        };

        let topic = self.document.exchange_topic(identity);

        let bytes = ecdh_encrypt(keypair, Some(identity), serde_json::to_vec(&request)?)?;

        let payload = PayloadBuilder::new(keypair, bytes)
            .from_ipfs(&self.ipfs)
            .await?;

        let bytes = payload.to_bytes()?;

        _ = self.ipfs.pubsub_publish(topic, bytes).await;

        self.ping_duration.insert(identity.clone(), Instant::now());
        self.pending_ping_response
            .insert(identity.clone(), Delay::new(Duration::from_millis(15)));

        Ok(())
    }

    async fn ping_all(&mut self) {
        let recipients = self.document.recipients();
        for identity in recipients {
            _ = self.ping(&identity).await;
        }
    }

    async fn send_single_conversation_event(
        &mut self,
        did_key: &DID,
        event: ConversationEvents,
    ) -> Result<(), Error> {
        let event = serde_json::to_vec(&event)?;

        let keypair = self.root.keypair();

        let bytes = ecdh_encrypt(keypair, Some(did_key), &event)?;

        let payload = PayloadBuilder::new(keypair, bytes)
            .from_ipfs(&self.ipfs)
            .await?;

        let peer_id = did_key.to_peer_id()?;
        let peers = self.ipfs.pubsub_peers(Some(did_key.messaging())).await?;

        let mut time = true;
        let timer = Instant::now();
        if !peers.contains(&peer_id)
            || (peers.contains(&peer_id)
                && self
                    .ipfs
                    .pubsub_publish(did_key.messaging(), payload.to_bytes()?)
                    .await
                    .is_err())
        {
            tracing::warn!(id=%&self.conversation_id, "Unable to publish to topic. Queuing event");
            self.queue_event(
                did_key.clone(),
                QueueItem::direct(
                    None,
                    peer_id,
                    did_key.messaging(),
                    payload.message().to_vec(),
                ),
            )
            .await;
            time = false;
        }
        if time {
            let end = timer.elapsed();
            tracing::info!(id=%self.conversation_id, "Event sent to {did_key}");
            tracing::trace!(id=%self.conversation_id, "Took {}ms to send event", end.as_millis());
        }

        Ok(())
    }

    pub async fn archived_conversation(&mut self) -> Result<(), Error> {
        let prev = self.document.archived;
        self.document.archived = true;
        self.set_document().await?;
        if !prev {
            self.event_subscription
                .emit(RayGunEventKind::ConversationArchived {
                    conversation_id: self.conversation_id,
                })
                .await;
        }
        Ok(())
    }

    pub async fn unarchived_conversation(&mut self) -> Result<(), Error> {
        let prev = self.document.archived;
        self.document.archived = false;
        self.set_document().await?;
        if prev {
            self.event_subscription
                .emit(RayGunEventKind::ConversationUnarchived {
                    conversation_id: self.conversation_id,
                })
                .await;
        }
        Ok(())
    }

    pub async fn update_conversation_permissions<P: Into<GroupPermissionOpt> + Send + Sync>(
        &mut self,
        permissions: P,
    ) -> Result<(), Error> {
        let own_did = self.identity.did_key();
        let Some(creator) = self.document.creator.as_ref() else {
            return Err(Error::InvalidConversation);
        };

        if creator != &own_did {
            return Err(Error::PublicKeyInvalid);
        }

        let permissions = match permissions.into() {
            GroupPermissionOpt::Map(permissions) => permissions,
            GroupPermissionOpt::Single((id, set)) => {
                let permissions = self.document.permissions.clone();
                {
                    let permissions = self.document.permissions.entry(id).or_default();
                    *permissions = set;
                }
                permissions
            }
        };

        let (added, removed) = self.document.permissions.compare_with_new(&permissions);

        self.document.permissions = permissions;
        self.set_document().await?;

        let event = MessagingEvents::UpdateConversation {
            conversation: self.document.clone(),
            kind: ConversationUpdateKind::ChangePermissions {
                permissions: self.document.permissions.clone(),
            },
        };

        let _ = self
            .event_broadcast
            .send(MessageEventKind::ConversationPermissionsUpdated {
                conversation_id: self.conversation_id,
                added,
                removed,
            });

        self.publish(None, event, true).await
    }

    async fn set_favorite_conversation(&mut self, favorite: bool) -> Result<(), Error> {
        self.document.favorite = favorite;
        self.set_document().await
    }

    async fn process_msg_event(&mut self, msg: Message) -> Result<(), Error> {
        let data = PayloadMessage::<Vec<u8>>::from_bytes(&msg.data)?;
        let sender = data.sender().to_did()?;

        let keypair = self.root.keypair();

        let own_did = keypair.to_did()?;

        let id = self.conversation_id;

        let bytes = match self.document.conversation_type() {
            ConversationType::Direct => {
                let list = self.document.recipients();

                let recipients = list
                    .iter()
                    .filter(|did| own_did.ne(did))
                    .collect::<Vec<_>>();

                let Some(member) = recipients.first() else {
                    tracing::warn!(id = %id, "participant is not in conversation");
                    return Err(Error::IdentityDoesntExist);
                };

                ecdh_decrypt(keypair, Some(member), data.message())?
            }
            ConversationType::Group => {
                let key = match self.keystore.get_latest(keypair, &sender) {
                    Ok(key) => key,
                    Err(Error::PublicKeyDoesntExist) => {
                        // If we are not able to get the latest key from the store, this is because we are still awaiting on the response from the key exchange
                        // So what we should so instead is set aside the payload until we receive the key exchange then attempt to process it again
                        _ = self.request_key(&sender).await;

                        // Note: We can set aside the data without the payload being owned directly due to the data already been verified
                        //       so we can own the data directly without worrying about the lifetime
                        //       however, we may want to eventually validate the data to ensure it havent been tampered in some way
                        //       while waiting for the response.

                        self.pending_key_exchange
                            .insert(sender, (data.message().to_vec(), false));

                        // Note: We will mark this as `Ok` since this is pending request to be resolved
                        return Ok(());
                    }
                    Err(e) => {
                        tracing::warn!(id = %id, sender = %data.sender(), error = %e, "Failed to obtain key");
                        return Err(e);
                    }
                };

                Cipher::direct_decrypt(data.message(), &key)?
            }
        };

        let event = serde_json::from_slice::<MessagingEvents>(&bytes).map_err(|e| {
            tracing::warn!(id = %id, sender = %data.sender(), error = %e, "Failed to deserialize message");
            e
        })?;

        message_event(self, &sender, event).await?;

        Ok(())
    }

    async fn messages_count(&self) -> Result<usize, Error> {
        self.document.messages_length(&self.ipfs).await
    }

    async fn get_message(&self, message_id: Uuid) -> Result<warp::raygun::Message, Error> {
        let keypair = self.root.keypair();

        let keystore = pubkey_or_keystore(self)?;

        self.document
            .get_message(&self.ipfs, keypair, message_id, keystore.as_ref())
            .await
    }

    async fn get_message_reference(&self, message_id: Uuid) -> Result<MessageReference, Error> {
        self.document
            .get_message_document(&self.ipfs, message_id)
            .await
            .map(|document| document.into())
    }

    async fn get_message_references<'a>(
        &self,
        opt: MessageOptions,
    ) -> Result<BoxStream<'a, MessageReference>, Error> {
        self.document
            .get_messages_reference_stream(&self.ipfs, opt)
            .await
    }

    pub async fn get_messages(&self, opt: MessageOptions) -> Result<Messages, Error> {
        let keypair = self.root.keypair();

        let keystore = pubkey_or_keystore(self)?;

        let m_type = opt.messages_type();
        match m_type {
            MessagesType::Stream => {
                let stream = self
                    .document
                    .get_messages_stream(&self.ipfs, keypair, opt, keystore)
                    .await?;
                Ok(Messages::Stream(stream))
            }
            MessagesType::List => {
                let list = self
                    .document
                    .get_messages(&self.ipfs, keypair, opt, keystore)
                    .await?;
                Ok(Messages::List(list))
            }
            MessagesType::Pages { .. } => {
                self.document
                    .get_messages_pages(&self.ipfs, keypair, opt, keystore.as_ref())
                    .await
            }
        }
    }

    fn conversation_key(&self, member: Option<&DID>) -> Result<Vec<u8>, Error> {
        let keypair = self.root.keypair();
        let own_did = self.identity.did_key();

        let conversation = &self.document;

        match conversation.conversation_type() {
            ConversationType::Direct => {
                let list = conversation.recipients();

                let recipients = list
                    .iter()
                    .filter(|did| own_did.ne(did))
                    .collect::<Vec<_>>();

                let member = recipients.first().ok_or(Error::InvalidConversation)?;
                ecdh_shared_key(keypair, Some(member))
            }
            ConversationType::Group => {
                let recipient = member.unwrap_or(&own_did);
                self.keystore.get_latest(keypair, recipient)
            }
        }
    }

    async fn request_key(&mut self, did: &DID) -> Result<(), Error> {
        if self.pending_key_request_sent.contains(did) {
            return Ok(());
        }

        let request = ConversationRequestResponse::Request {
            conversation_id: self.conversation_id,
            kind: ConversationRequestKind::Key,
        };

        let conversation = &self.document;

        if !conversation.recipients().contains(did) {
            //TODO: user is not a recipient of the conversation
            return Err(Error::PublicKeyInvalid);
        }

        let keypair = self.root.keypair();

        let bytes = ecdh_encrypt(keypair, Some(did), serde_json::to_vec(&request)?)?;

        let payload = PayloadBuilder::new(keypair, bytes)
            .from_ipfs(&self.ipfs)
            .await?;

        let topic = conversation.exchange_topic(did);

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
            tracing::warn!(id = %self.conversation_id, "Unable to publish to topic");
            self.queue_event(
                did.clone(),
                QueueItem::direct(None, peer_id, topic.clone(), payload.message().to_vec()),
            )
            .await;
        }

        // TODO: Store request locally and hold any messages and events until key is received from peer
        self.pending_key_request_sent.insert(did.clone());

        Ok(())
    }

    //TODO: Send a request to recipient(s) of the chat to ack if message been delivered if message is marked "sent" unless we receive an event acknowledging the message itself
    //Note:
    //  - For group chat, this can be ignored unless we decide to have a full acknowledgement from all recipients in which case, we can mark it as "sent"
    //    until all confirm to have received the message
    //  - If member sends an event stating that they do not have the message to grab the message from the store
    //    and send it them, with a map marking the attempt(s)
    async fn message_status(&self, message_id: Uuid) -> Result<MessageStatus, Error> {
        if matches!(self.document.conversation_type(), ConversationType::Group) {
            //TODO: Handle message status for group
            return Err(Error::Unimplemented);
        }

        let messages = self.document.get_message_list(&self.ipfs).await?;

        if !messages.iter().any(|document| document.id == message_id) {
            return Err(Error::MessageNotFound);
        }

        let own_did = self.identity.did_key();

        let _list = self
            .document
            .recipients()
            .iter()
            .filter(|did| own_did.ne(did))
            .cloned()
            .collect::<Vec<_>>();

        // TODO:
        // for peer in list {
        //     if let Some(list) = self.queue.get(&peer) {
        //         for item in list {
        //             let Queue { id, m_id, .. } = item;
        //             if self.document.id() == *id {
        //                 if let Some(m_id) = m_id {
        //                     if message_id == *m_id {
        //                         return Ok(MessageStatus::NotSent);
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // }

        //Not a guarantee that it been sent but for now since the message exist locally and not marked in queue, we will assume it have been sent
        Ok(MessageStatus::Sent)
    }

    pub async fn send_message(&mut self, messages: Vec<String>) -> Result<Uuid, Error> {
        if messages.is_empty() {
            return Err(Error::EmptyMessage);
        }

        let lines_value_length: usize = messages
            .iter()
            .filter(|s| !s.is_empty())
            .map(|s| s.trim())
            .map(|s| s.chars().count())
            .sum();

        if lines_value_length == 0 || lines_value_length > MAX_MESSAGE_SIZE {
            tracing::error!(
                current_size = lines_value_length,
                max = MAX_MESSAGE_SIZE,
                "length of message is invalid"
            );
            return Err(Error::InvalidLength {
                context: "message".into(),
                current: lines_value_length,
                minimum: Some(MIN_MESSAGE_SIZE),
                maximum: Some(MAX_MESSAGE_SIZE),
            });
        }

        let keypair = self.root.keypair();
        let own_did = self.identity.did_key();

        let mut message = warp::raygun::Message::default();
        message.set_conversation_id(self.conversation_id);
        message.set_sender(own_did.clone());
        message.set_lines(messages.clone());

        let message_id = message.id();
        let keystore = pubkey_or_keystore(&*self)?;

        let message = MessageDocument::new(&self.ipfs, keypair, message, keystore.as_ref()).await?;

        let message_cid = self
            .document
            .insert_message_document(&self.ipfs, &message)
            .await?;

        let recipients = self.document.recipients();

        self.set_document().await?;

        let event = MessageEventKind::MessageSent {
            conversation_id: self.conversation_id,
            message_id,
        };

        if let Err(e) = self.event_broadcast.clone().send(event) {
            tracing::error!(conversation_id=%self.conversation_id, error = %e, "Error broadcasting event");
        }

        let message_id = message.id;

        let event = MessagingEvents::New { message };

        if !recipients.is_empty() {
            if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
                for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
                    let _ = self
                        .message_command
                        .clone()
                        .send(MessageCommand::InsertMessage {
                            peer_id,
                            conversation_id: self.conversation_id,
                            recipients: recipients.clone(),
                            message_id,
                            message_cid,
                        })
                        .await;
                }
            }
        }

        self.publish(Some(message_id), event, true)
            .await
            .map(|_| message_id)
    }

    pub async fn edit_message(
        &mut self,
        message_id: Uuid,
        messages: Vec<String>,
    ) -> Result<(), Error> {
        let tx = self.event_broadcast.clone();

        if messages.is_empty() {
            return Err(Error::EmptyMessage);
        }

        let lines_value_length: usize = messages
            .iter()
            .filter(|s| !s.is_empty())
            .map(|s| s.trim())
            .map(|s| s.chars().count())
            .sum();

        if lines_value_length == 0 || lines_value_length > MAX_MESSAGE_SIZE {
            tracing::error!(
                current_size = lines_value_length,
                max = MAX_MESSAGE_SIZE,
                "length of message is invalid"
            );
            return Err(Error::InvalidLength {
                context: "message".into(),
                current: lines_value_length,
                minimum: Some(MIN_MESSAGE_SIZE),
                maximum: Some(MAX_MESSAGE_SIZE),
            });
        }

        let keypair = self.root.keypair();

        let keystore = pubkey_or_keystore(&*self)?;

        let mut message_document = self
            .document
            .get_message_document(&self.ipfs, message_id)
            .await?;

        let mut message = message_document
            .resolve(&self.ipfs, keypair, true, keystore.as_ref())
            .await?;

        let sender = message.sender();

        let own_did = &self.identity.did_key();

        if sender.ne(own_did) {
            return Err(Error::InvalidMessage);
        }

        message.lines_mut().clone_from(&messages);
        message.set_modified(Utc::now());

        message_document
            .update(&self.ipfs, keypair, message, None, keystore.as_ref(), None)
            .await?;

        let nonce = message_document.nonce_from_message()?;
        let signature = message_document.signature.expect("message to be signed");

        let message_cid = self
            .document
            .update_message_document(&self.ipfs, &message_document)
            .await?;

        let recipients = self.document.recipients();

        self.set_document().await?;

        let _ = tx.send(MessageEventKind::MessageEdited {
            conversation_id: self.conversation_id,
            message_id,
        });

        let event = MessagingEvents::Edit {
            conversation_id: self.conversation_id,
            message_id,
            modified: message_document.modified.expect("message to be modified"),
            lines: messages,
            nonce: nonce.to_vec(),
            signature: signature.into(),
        };

        if !recipients.is_empty() {
            if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
                for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
                    let _ = self
                        .message_command
                        .clone()
                        .send(MessageCommand::InsertMessage {
                            peer_id,
                            conversation_id: self.conversation_id,
                            recipients: recipients.clone(),
                            message_id,
                            message_cid,
                        })
                        .await;
                }
            }
        }

        self.publish(None, event, true).await
    }

    pub async fn reply_message(
        &mut self,
        message_id: Uuid,
        messages: Vec<String>,
    ) -> Result<Uuid, Error> {
        let tx = self.event_broadcast.clone();

        if messages.is_empty() {
            return Err(Error::EmptyMessage);
        }

        let lines_value_length: usize = messages
            .iter()
            .filter(|s| !s.is_empty())
            .map(|s| s.trim())
            .map(|s| s.chars().count())
            .sum();

        if lines_value_length == 0 || lines_value_length > MAX_MESSAGE_SIZE {
            tracing::error!(
                current_size = lines_value_length,
                max = MAX_MESSAGE_SIZE,
                "length of message is invalid"
            );
            return Err(Error::InvalidLength {
                context: "message".into(),
                current: lines_value_length,
                minimum: Some(MIN_MESSAGE_SIZE),
                maximum: Some(MAX_MESSAGE_SIZE),
            });
        }

        let keypair = self.root.keypair();

        let own_did = self.identity.did_key();

        let mut message = warp::raygun::Message::default();
        message.set_conversation_id(self.conversation_id);
        message.set_sender(own_did.clone());
        message.set_lines(messages);
        message.set_replied(Some(message_id));

        let keystore = pubkey_or_keystore(&*self)?;

        let message = MessageDocument::new(&self.ipfs, keypair, message, keystore.as_ref()).await?;

        let message_id = message.id;

        let message_cid = self
            .document
            .insert_message_document(&self.ipfs, &message)
            .await?;

        let recipients = self.document.recipients();

        self.set_document().await?;

        let event = MessageEventKind::MessageSent {
            conversation_id: self.conversation_id,
            message_id,
        };

        if let Err(e) = tx.send(event) {
            tracing::error!(id=%self.conversation_id, error = %e, "Error broadcasting event");
        }

        let event = MessagingEvents::New { message };

        if !recipients.is_empty() {
            if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
                for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
                    let _ = self
                        .message_command
                        .clone()
                        .send(MessageCommand::InsertMessage {
                            peer_id,
                            conversation_id: self.conversation_id,
                            recipients: recipients.clone(),
                            message_id,
                            message_cid,
                        })
                        .await;
                }
            }
        }

        self.publish(Some(message_id), event, true)
            .await
            .map(|_| message_id)
    }

    pub async fn delete_message(&mut self, message_id: Uuid, broadcast: bool) -> Result<(), Error> {
        let tx = self.event_broadcast.clone();

        let event = MessagingEvents::Delete {
            conversation_id: self.conversation_id,
            message_id,
        };

        self.document.delete_message(&self.ipfs, message_id).await?;

        self.set_document().await?;

        if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
            for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
                let _ = self
                    .message_command
                    .clone()
                    .send(MessageCommand::RemoveMessage {
                        peer_id,
                        conversation_id: self.conversation_id,
                        message_id,
                    })
                    .await;
            }
        }

        let _ = tx.send(MessageEventKind::MessageDeleted {
            conversation_id: self.conversation_id,
            message_id,
        });

        if broadcast {
            self.publish(None, event, true).await?;
        }

        Ok(())
    }

    pub async fn pin_message(&mut self, message_id: Uuid, state: PinState) -> Result<(), Error> {
        let tx = self.event_broadcast.clone();

        let keypair = self.root.keypair();
        let own_did = self.identity.did_key();

        let keystore = pubkey_or_keystore(&*self)?;

        let mut message_document = self
            .document
            .get_message_document(&self.ipfs, message_id)
            .await?;

        let mut message = message_document
            .resolve(&self.ipfs, keypair, true, keystore.as_ref())
            .await?;

        let event = match state {
            PinState::Pin => {
                if message.pinned() {
                    return Ok(());
                }
                *message.pinned_mut() = true;
                MessageEventKind::MessagePinned {
                    conversation_id: self.conversation_id,
                    message_id,
                }
            }
            PinState::Unpin => {
                if !message.pinned() {
                    return Ok(());
                }
                *message.pinned_mut() = false;
                MessageEventKind::MessageUnpinned {
                    conversation_id: self.conversation_id,
                    message_id,
                }
            }
        };

        message_document
            .update(&self.ipfs, keypair, message, None, keystore.as_ref(), None)
            .await?;

        let message_cid = self
            .document
            .update_message_document(&self.ipfs, &message_document)
            .await?;

        let recipients = self.document.recipients();

        self.set_document().await?;

        let _ = tx.send(event);

        if !recipients.is_empty() {
            if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
                for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
                    let _ = self
                        .message_command
                        .clone()
                        .send(MessageCommand::InsertMessage {
                            peer_id,
                            conversation_id: self.conversation_id,
                            recipients: recipients.clone(),
                            message_id,
                            message_cid,
                        })
                        .await;
                }
            }
        }

        let event = MessagingEvents::Pin {
            conversation_id: self.conversation_id,
            member: own_did,
            message_id,
            state,
        };

        self.publish(None, event, true).await
    }

    pub async fn react(
        &mut self,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
    ) -> Result<(), Error> {
        let tx = self.event_broadcast.clone();

        let keypair = self.root.keypair();

        let own_did = self.identity.did_key();

        let keystore = pubkey_or_keystore(&*self)?;

        let mut message_document = self
            .document
            .get_message_document(&self.ipfs, message_id)
            .await?;

        let mut message = message_document
            .resolve(&self.ipfs, keypair, true, keystore.as_ref())
            .await?;

        let recipients = self.document.recipients();

        let reactions = message.reactions_mut();

        let message_cid;

        match state {
            ReactionState::Add => {
                if reactions.len() >= MAX_REACTIONS {
                    return Err(Error::InvalidLength {
                        context: "reactions".into(),
                        current: reactions.len(),
                        minimum: None,
                        maximum: Some(MAX_REACTIONS),
                    });
                }

                let entry = reactions.entry(emoji.clone()).or_default();

                if entry.contains(&own_did) {
                    return Err(Error::ReactionExist);
                }

                entry.push(own_did.clone());

                message_document
                    .update(&self.ipfs, keypair, message, None, keystore.as_ref(), None)
                    .await?;

                message_cid = self
                    .document
                    .update_message_document(&self.ipfs, &message_document)
                    .await?;
                self.set_document().await?;

                _ = tx.send(MessageEventKind::MessageReactionAdded {
                    conversation_id: self.conversation_id,
                    message_id,
                    did_key: own_did.clone(),
                    reaction: emoji.clone(),
                });
            }
            ReactionState::Remove => {
                match reactions.entry(emoji.clone()) {
                    indexmap::map::Entry::Occupied(mut e) => {
                        let list = e.get_mut();

                        if !list.contains(&own_did) {
                            return Err(Error::ReactionDoesntExist);
                        }

                        list.retain(|did| did != &own_did);
                        if list.is_empty() {
                            e.swap_remove();
                        }
                    }
                    indexmap::map::Entry::Vacant(_) => return Err(Error::ReactionDoesntExist),
                };

                message_document
                    .update(&self.ipfs, keypair, message, None, keystore.as_ref(), None)
                    .await?;

                message_cid = self
                    .document
                    .update_message_document(&self.ipfs, &message_document)
                    .await?;

                self.set_document().await?;

                let _ = tx.send(MessageEventKind::MessageReactionRemoved {
                    conversation_id: self.conversation_id,
                    message_id,
                    did_key: own_did.clone(),
                    reaction: emoji.clone(),
                });
            }
        }

        let event = MessagingEvents::React {
            conversation_id: self.conversation_id,
            reactor: own_did,
            message_id,
            state,
            emoji,
        };

        if !recipients.is_empty() {
            if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
                for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
                    let _ = self
                        .message_command
                        .clone()
                        .send(MessageCommand::InsertMessage {
                            peer_id,
                            conversation_id: self.conversation_id,
                            recipients: recipients.clone(),
                            message_id,
                            message_cid,
                        })
                        .await;
                }
            }
        }

        self.publish(None, event, true).await
    }

    pub async fn send_event(&self, event: MessageEvent) -> Result<(), Error> {
        let conversation_id = self.conversation_id;
        let member = self.identity.did_key();

        let event = MessagingEvents::Event {
            conversation_id,
            member,
            event,
            cancelled: false,
        };
        self.send_message_event(event).await
    }

    pub async fn cancel_event(&self, event: MessageEvent) -> Result<(), Error> {
        let member = self.identity.did_key();
        let conversation_id = self.conversation_id;
        let event = MessagingEvents::Event {
            conversation_id,
            member,
            event,
            cancelled: true,
        };
        self.send_message_event(event).await
    }

    pub async fn send_message_event(&self, event: MessagingEvents) -> Result<(), Error> {
        let event = serde_json::to_vec(&event)?;

        let key = self.conversation_key(None)?;

        let bytes = Cipher::direct_encrypt(&event, &key)?;

        let payload = PayloadBuilder::new(self.root.keypair(), bytes)
            .from_ipfs(&self.ipfs)
            .await?;

        let peers = self
            .ipfs
            .pubsub_peers(Some(self.document.event_topic()))
            .await?;

        if !peers.is_empty() {
            if let Err(e) = self
                .ipfs
                .pubsub_publish(self.document.event_topic(), payload.to_bytes()?)
                .await
            {
                tracing::error!(id=%self.conversation_id, "Unable to send event: {e}");
            }
        }
        Ok(())
    }

    pub async fn add_participant(&mut self, did_key: &DID) -> Result<(), Error> {
        if let ConversationType::Direct = self.document.conversation_type() {
            return Err(Error::InvalidConversation);
        }
        assert_eq!(self.document.conversation_type(), ConversationType::Group);

        let Some(creator) = self.document.creator.clone() else {
            return Err(Error::InvalidConversation);
        };

        let own_did = &self.identity.did_key();

        if !self
            .document
            .permissions
            .has_permission(own_did, GroupPermission::AddParticipants)
            && creator.ne(own_did)
        {
            return Err(Error::PublicKeyInvalid);
        }

        if creator.eq(did_key) {
            return Err(Error::PublicKeyInvalid);
        }

        if self.root.is_blocked(did_key).await? {
            return Err(Error::PublicKeyIsBlocked);
        }

        if self.document.restrict.contains(did_key) {
            return Err(Error::PublicKeyIsBlocked);
        }

        if self.document.recipients.contains(did_key) {
            return Err(Error::IdentityExist);
        }

        self.document.recipients.push(did_key.clone());

        self.set_document().await?;

        let event = MessagingEvents::UpdateConversation {
            conversation: self.document.clone(),
            kind: ConversationUpdateKind::AddParticipant {
                did: did_key.clone(),
            },
        };

        let tx = self.event_broadcast.clone();
        let _ = tx.send(MessageEventKind::RecipientAdded {
            conversation_id: self.conversation_id,
            recipient: did_key.clone(),
        });

        self.publish(None, event, true).await?;

        let new_event = ConversationEvents::NewGroupConversation {
            conversation: self.document.clone(),
        };

        self.send_single_conversation_event(did_key, new_event)
            .await?;
        if let Err(_e) = self.ping(did_key).await {}
        Ok(())
    }

    pub async fn remove_participant(
        &mut self,
        did_key: &DID,
        broadcast: bool,
    ) -> Result<(), Error> {
        if matches!(self.document.conversation_type(), ConversationType::Direct) {
            return Err(Error::InvalidConversation);
        }

        let Some(creator) = self.document.creator.as_ref() else {
            return Err(Error::InvalidConversation);
        };

        let own_did = &self.identity.did_key();

        if creator.ne(own_did) {
            return Err(Error::PublicKeyInvalid);
        }

        if creator.eq(did_key) {
            return Err(Error::PublicKeyInvalid);
        }

        if !self.document.recipients.contains(did_key) {
            return Err(Error::IdentityDoesntExist);
        }

        self.document.recipients.retain(|did| did.ne(did_key));
        self.set_document().await?;

        let event = MessagingEvents::UpdateConversation {
            conversation: self.document.clone(),
            kind: ConversationUpdateKind::RemoveParticipant {
                did: did_key.clone(),
            },
        };

        let tx = self.event_broadcast.clone();
        let _ = tx.send(MessageEventKind::RecipientRemoved {
            conversation_id: self.conversation_id,
            recipient: did_key.clone(),
        });

        self.pending_ping_response.remove(did_key);
        self.ping_duration.shift_remove(did_key);

        self.publish(None, event, true).await?;

        if broadcast {
            let new_event = ConversationEvents::DeleteConversation {
                conversation_id: self.conversation_id,
            };

            self.send_single_conversation_event(did_key, new_event)
                .await?;
        }

        Ok(())
    }

    pub async fn add_restricted(&mut self, did_key: &DID) -> Result<(), Error> {
        if matches!(self.document.conversation_type(), ConversationType::Direct) {
            return Err(Error::InvalidConversation);
        }

        let Some(creator) = self.document.creator.clone() else {
            return Err(Error::InvalidConversation);
        };

        let own_did = &self.identity.did_key();

        if creator.ne(own_did) {
            return Err(Error::PublicKeyInvalid);
        }

        if creator.eq(did_key) {
            return Err(Error::PublicKeyInvalid);
        }

        if !self.root.is_blocked(did_key).await? {
            return Err(Error::PublicKeyIsntBlocked);
        }

        debug_assert!(!self.document.recipients.contains(did_key));
        debug_assert!(!self.document.restrict.contains(did_key));

        self.document.restrict.push(did_key.clone());

        self.set_document().await?;

        let event = MessagingEvents::UpdateConversation {
            conversation: self.document.clone(),
            kind: ConversationUpdateKind::AddRestricted {
                did: did_key.clone(),
            },
        };

        self.publish(None, event, true).await
    }

    pub async fn remove_restricted(&mut self, did_key: &DID) -> Result<(), Error> {
        if matches!(self.document.conversation_type(), ConversationType::Direct) {
            return Err(Error::InvalidConversation);
        }

        let Some(creator) = self.document.creator.clone() else {
            return Err(Error::InvalidConversation);
        };

        let own_did = &self.identity.did_key();

        if creator.ne(own_did) {
            return Err(Error::PublicKeyInvalid);
        }

        if creator.eq(did_key) {
            return Err(Error::PublicKeyInvalid);
        }

        if self.root.is_blocked(did_key).await? {
            return Err(Error::PublicKeyIsBlocked);
        }

        debug_assert!(self.document.restrict.contains(did_key));

        self.document
            .restrict
            .retain(|restricted| restricted != did_key);

        self.set_document().await?;

        let event = MessagingEvents::UpdateConversation {
            conversation: self.document.clone(),
            kind: ConversationUpdateKind::RemoveRestricted {
                did: did_key.clone(),
            },
        };

        self.publish(None, event, true).await
    }

    pub async fn update_conversation_name(&mut self, name: &str) -> Result<(), Error> {
        let name = name.trim();
        let name_length = name.len();

        if name_length > 255 {
            return Err(Error::InvalidLength {
                context: "name".into(),
                current: name_length,
                minimum: None,
                maximum: Some(255),
            });
        }

        if let ConversationType::Direct = self.document.conversation_type() {
            return Err(Error::InvalidConversation);
        }
        assert_eq!(self.document.conversation_type(), ConversationType::Group);

        let Some(creator) = self.document.creator.clone() else {
            return Err(Error::InvalidConversation);
        };

        let own_did = &self.identity.did_key();

        if !&self
            .document
            .permissions
            .has_permission(own_did, GroupPermission::SetGroupName)
            && creator.ne(own_did)
        {
            return Err(Error::PublicKeyInvalid);
        }

        self.document.name = (!name.is_empty()).then_some(name.to_string());

        self.set_document().await?;

        let new_name = self.document.name();

        let event = MessagingEvents::UpdateConversation {
            conversation: self.document.clone(),
            kind: ConversationUpdateKind::ChangeName { name: new_name },
        };

        let _ = self
            .event_broadcast
            .send(MessageEventKind::ConversationNameUpdated {
                conversation_id: self.conversation_id,
                name: name.to_string(),
            });

        self.publish(None, event, true).await
    }

    pub async fn conversation_image(
        &self,
        image_type: ConversationImageType,
    ) -> Result<ConversationImage, Error> {
        let (cid, max_size) = match image_type {
            ConversationImageType::Icon => {
                let cid = self.document.icon.ok_or(Error::Other)?;
                (cid, MAX_CONVERSATION_ICON_SIZE)
            }
            ConversationImageType::Banner => {
                let cid = self.document.banner.ok_or(Error::Other)?;
                (cid, MAX_CONVERSATION_BANNER_SIZE)
            }
        };

        let dag: ImageDag = self.ipfs.get_dag(cid).deserialized().await?;

        if dag.size > max_size as _ {
            return Err(Error::InvalidLength {
                context: "image".into(),
                current: dag.size as _,
                minimum: None,
                maximum: Some(max_size),
            });
        }

        let image = self
            .ipfs
            .cat_unixfs(dag.link)
            .max_length(dag.size as _)
            .await
            .map_err(anyhow::Error::from)?;

        let mut img = ConversationImage::default();
        img.set_image_type(dag.mime);
        img.set_data(image.into());
        Ok(img)
    }

    pub async fn update_conversation_image(
        &mut self,
        location: Location,
        image_type: ConversationImageType,
    ) -> Result<(), Error> {
        let max_size = match image_type {
            ConversationImageType::Banner => MAX_CONVERSATION_BANNER_SIZE,
            ConversationImageType::Icon => MAX_CONVERSATION_ICON_SIZE,
        };

        let own_did = self.identity.did_key();

        if self.document.conversation_type() == ConversationType::Group
            && !matches!(self.document.creator.as_ref(), Some(creator) if own_did.eq(creator))
        {
            return Err(Error::InvalidConversation);
        }

        let (cid, size, ext) = match location {
            Location::Constellation { path } => {
                let file = self
                    .file
                    .root_directory()
                    .get_item_by_path(&path)
                    .and_then(|item| item.get_file())?;

                let extension = file.file_type();

                if file.size() > max_size {
                    return Err(Error::InvalidLength {
                        context: "image".into(),
                        current: file.size(),
                        minimum: Some(1),
                        maximum: Some(max_size),
                    });
                }

                let document = FileDocument::new(&self.ipfs, &file).await?;
                let cid = document
                    .reference
                    .as_ref()
                    .and_then(|reference| IpfsPath::from_str(reference).ok())
                    .and_then(|path| path.root().cid().copied())
                    .ok_or(Error::OtherWithContext("invalid reference".into()))?;

                (cid, document.size, extension)
            }
            Location::Disk { path } => {
                #[cfg(target_arch = "wasm32")]
                {
                    _ = path;
                    unreachable!()
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    use crate::utils::ReaderStream;
                    use tokio_util::compat::TokioAsyncReadCompatExt;

                    let extension = path
                        .extension()
                        .and_then(OsStr::to_str)
                        .map(ExtensionType::from)
                        .unwrap_or(ExtensionType::Other)
                        .into();

                    let file = tokio::fs::File::open(path).await?;
                    let size = file.metadata().await?.len() as _;
                    let stream =
                        ReaderStream::from_reader_with_cap(file.compat(), 512, Some(max_size))
                            .boxed();
                    let path = self.ipfs.add_unixfs(stream).pin(false).await?;
                    let cid = path.root().cid().copied().expect("valid cid in path");
                    (cid, size, extension)
                }
            }
            Location::Stream {
                // NOTE: `name` and `size` would not be used here as we are only storing the data. If we are to store in constellation too, we would make use of these fields
                name: _,
                size: _,
                stream,
            } => {
                let bytes = ByteCollection::new_with_max_capacity(stream, max_size).await?;

                let bytes_len = bytes.len();

                let path = self.ipfs.add_unixfs(bytes.clone()).pin(false).await?;
                let cid = path.root().cid().copied().expect("valid cid in path");

                let cursor = std::io::Cursor::new(bytes);

                let image = image::ImageReader::new(cursor).with_guessed_format()?;

                let format = image
                    .format()
                    .and_then(|format| ExtensionType::try_from(format).ok())
                    .unwrap_or(ExtensionType::Other)
                    .into();

                (cid, bytes_len, format)
            }
        };

        let dag = ImageDag {
            link: cid,
            size: size as _,
            mime: ext,
        };

        let cid = self.ipfs.put_dag(dag).await?;

        let kind = match image_type {
            ConversationImageType::Icon => {
                self.document.icon.replace(cid);
                ConversationUpdateKind::AddedIcon
            }
            ConversationImageType::Banner => {
                self.document.banner.replace(cid);
                ConversationUpdateKind::AddedBanner
            }
        };

        self.set_document().await?;

        let event = MessagingEvents::UpdateConversation {
            conversation: self.document.clone(),
            kind,
        };

        let message_event = match image_type {
            ConversationImageType::Icon => MessageEventKind::ConversationUpdatedIcon {
                conversation_id: self.conversation_id,
            },
            ConversationImageType::Banner => MessageEventKind::ConversationUpdatedBanner {
                conversation_id: self.conversation_id,
            },
        };

        let _ = self.event_broadcast.send(message_event);

        self.publish(None, event, true).await
    }

    pub async fn remove_conversation_image(
        &mut self,
        image_type: ConversationImageType,
    ) -> Result<(), Error> {
        let own_did = self.identity.did_key();

        if self.document.conversation_type() == ConversationType::Group
            && !matches!(self.document.creator.as_ref(), Some(creator) if own_did.eq(creator))
        {
            return Err(Error::InvalidConversation);
        }

        let cid = match image_type {
            ConversationImageType::Icon => self.document.icon.take(),
            ConversationImageType::Banner => self.document.banner.take(),
        };

        if cid.is_none() {
            return Err(Error::ObjectNotFound); //TODO: conversation image doesnt exist
        }

        self.set_document().await?;

        let kind = match image_type {
            ConversationImageType::Icon => ConversationUpdateKind::RemovedIcon,
            ConversationImageType::Banner => ConversationUpdateKind::RemovedBanner,
        };

        let event = MessagingEvents::UpdateConversation {
            conversation: self.document.clone(),
            kind,
        };

        let conversation_id = self.conversation_id;

        let message_event = match image_type {
            ConversationImageType::Icon => {
                MessageEventKind::ConversationUpdatedIcon { conversation_id }
            }
            ConversationImageType::Banner => {
                MessageEventKind::ConversationUpdatedBanner { conversation_id }
            }
        };

        let _ = self.event_broadcast.send(message_event);

        self.publish(None, event, true).await
    }

    pub async fn set_description(&mut self, desc: Option<&str>) -> Result<(), Error> {
        let conversation_id = self.conversation_id;
        let own_did = &self.identity.did_key();

        if self.document.conversation_type() == ConversationType::Group {
            let Some(creator) = self.document.creator.as_ref() else {
                return Err(Error::InvalidConversation);
            };
            if own_did != creator {
                return Err(Error::InvalidConversation); //TODO:
            }
        }

        if let Some(desc) = desc {
            if desc.is_empty() || desc.len() > MAX_CONVERSATION_DESCRIPTION {
                return Err(Error::InvalidLength {
                    context: "description".into(),
                    minimum: Some(1),
                    maximum: Some(MAX_CONVERSATION_DESCRIPTION),
                    current: desc.len(),
                });
            }
        }

        self.document.description = desc.map(ToString::to_string);

        self.set_document().await?;

        let ev = MessageEventKind::ConversationDescriptionChanged {
            conversation_id,
            description: desc.map(ToString::to_string),
        };

        let _ = self.event_broadcast.send(ev);

        let event = MessagingEvents::UpdateConversation {
            conversation: self.document.clone(),
            kind: ConversationUpdateKind::ChangeDescription {
                description: desc.map(ToString::to_string),
            },
        };

        self.publish(None, event, true).await
    }

    pub fn attach(
        &mut self,
        reply_id: Option<Uuid>,
        locations: Vec<Location>,
        messages: Vec<String>,
    ) -> Result<(Uuid, AttachmentEventStream), Error> {
        let conversation_id = self.conversation_id;
        if locations.len() > 32 {
            return Err(Error::InvalidLength {
                context: "files".into(),
                current: locations.len(),
                minimum: Some(1),
                maximum: Some(32),
            });
        }

        if !messages.is_empty() {
            let lines_value_length: usize = messages
                .iter()
                .filter(|s| !s.is_empty())
                .map(|s| s.trim())
                .map(|s| s.chars().count())
                .sum();

            if lines_value_length > MAX_MESSAGE_SIZE {
                tracing::error!(
                    current_size = lines_value_length,
                    max = MAX_MESSAGE_SIZE,
                    "length of message is invalid"
                );
                return Err(Error::InvalidLength {
                    context: "message".into(),
                    current: lines_value_length,
                    minimum: None,
                    maximum: Some(MAX_MESSAGE_SIZE),
                });
            }
        }

        let keypair = self.root.keypair();

        let mut constellation = self.file.clone();

        let files = locations
            .into_iter()
            .filter(|location| match location {
                Location::Disk { path } => path.is_file(),
                _ => true,
            })
            .collect::<Vec<_>>();

        if files.is_empty() {
            return Err(Error::NoAttachments);
        }

        let root_directory = constellation.root_directory();

        if !root_directory.has_item(CHAT_DIRECTORY) {
            let new_dir = Directory::new(CHAT_DIRECTORY);
            root_directory.add_directory(new_dir)?;
        }

        let mut media_dir = root_directory
            .get_last_directory_from_path(&format!("/{CHAT_DIRECTORY}/{conversation_id}"))?;

        // if the directory that returned is the chat directory, this means we should create
        // the directory specific to the conversation
        if media_dir.name() == CHAT_DIRECTORY {
            let new_dir = Directory::new(&conversation_id.to_string());
            media_dir.add_directory(new_dir)?;
            media_dir = media_dir.get_last_directory_from_path(&conversation_id.to_string())?;
        }

        assert_eq!(media_dir.name(), conversation_id.to_string());

        let mut atx = self.attachment_tx.clone();
        let keystore = pubkey_or_keystore(&*self)?;
        let ipfs = self.ipfs.clone();
        let own_did = self.identity.did_key();

        let keypair = keypair.clone();

        let message_id = Uuid::new_v4();

        let stream = async_stream::stream! {
            let mut in_stack = vec![];

            let mut attachments = vec![];

            let mut streams = StreamMap::new();

            for file in files {
                let kind = LocationKind::from(&file);
                match file {
                    Location::Constellation { path } => {
                        match constellation
                            .root_directory()
                            .get_item_by_path(&path)
                            .and_then(|item| item.get_file())
                        {
                            Ok(f) => {
                                streams.insert(kind, stream::once(async { (Progression::ProgressComplete { name: f.name(), total: Some(f.size()) }, Some(f)) }).boxed());
                            },
                            Err(e) => {
                                let constellation_path = PathBuf::from(&path);
                                let name = constellation_path.file_name().and_then(OsStr::to_str).map(str::to_string).unwrap_or(path.to_string());
                                streams.insert(kind, stream::once(async { (Progression::ProgressFailed { name, last_size: None, error: e }, None) }).boxed());
                            },
                        }
                    }
                    Location::Stream { name, size, stream } => {
                        let mut filename = name;

                        let original = filename.clone();

                        let current_directory = media_dir.clone();

                        let mut interval = 0;
                        let skip;
                        loop {
                            if in_stack.contains(&filename) || current_directory.has_item(&filename) {
                                if interval > 2000 {
                                    skip = true;
                                    break;
                                }
                                interval += 1;
                                let file = PathBuf::from(&original);
                                let file_stem =
                                    file.file_stem().and_then(OsStr::to_str).map(str::to_string);
                                let ext = file.extension().and_then(OsStr::to_str).map(str::to_string);

                                filename = match (file_stem, ext) {
                                    (Some(filename), Some(ext)) => {
                                        format!("{filename} ({interval}).{ext}")
                                    }
                                    _ => format!("{original} ({interval})"),
                                };
                                continue;
                            }
                            skip = false;
                            break;
                        }

                        if skip {
                            streams.insert(kind, stream::once(async { (Progression::ProgressFailed { name: filename, last_size: None, error: Error::InvalidFile }, None) }).boxed());
                            continue;
                        }

                        in_stack.push(filename.clone());

                        let filename = format!("/{CHAT_DIRECTORY}/{conversation_id}/{filename}");

                        let mut progress = match constellation.put_stream(&filename, size, stream).await {
                            Ok(stream) => stream,
                            Err(e) => {
                               tracing::error!(%conversation_id, "Error uploading {filename}: {e}");
                                streams.insert(kind, stream::once(async { (Progression::ProgressFailed { name: filename, last_size: None, error: e }, None) }).boxed());
                                continue;
                            }
                        };


                        let directory = root_directory.clone();
                        let filename = filename.to_string();

                        let stream = async_stream::stream! {
                            while let Some(item) = progress.next().await {
                                match item {
                                    item @ Progression::CurrentProgress { .. } => {
                                        yield (item, None);
                                    },
                                    item @ Progression::ProgressComplete { .. } => {
                                        let file_name = directory.get_item_by_path(&filename).and_then(|item| item.get_file()).ok();
                                        yield (item, file_name);
                                        break;
                                    },
                                    item @ Progression::ProgressFailed { .. } => {
                                        yield (item, None);
                                        break;
                                    }
                                }
                            }
                        };

                        streams.insert(kind, stream.boxed());
                    }
                    Location::Disk { path } => {
                        #[cfg(target_arch = "wasm32")]
                        {
                            _ = path;
                            unreachable!()
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        {
                            let mut filename = match path.file_name() {
                                Some(file) => file.to_string_lossy().to_string(),
                                None => continue,
                            };

                            let original = filename.clone();

                            let current_directory = media_dir.clone();

                            let mut interval = 0;
                            let skip;
                            loop {
                                if in_stack.contains(&filename) || current_directory.has_item(&filename) {
                                    if interval > 2000 {
                                        skip = true;
                                        break;
                                    }
                                    interval += 1;
                                    let file = PathBuf::from(&original);
                                    let file_stem =
                                        file.file_stem().and_then(OsStr::to_str).map(str::to_string);
                                    let ext = file.extension().and_then(OsStr::to_str).map(str::to_string);

                                    filename = match (file_stem, ext) {
                                        (Some(filename), Some(ext)) => {
                                            format!("{filename} ({interval}).{ext}")
                                        }
                                        _ => format!("{original} ({interval})"),
                                    };
                                    continue;
                                }
                                skip = false;
                                break;
                            }

                            if skip {
                                streams.insert(kind, stream::once(async { (Progression::ProgressFailed { name: filename, last_size: None, error: Error::InvalidFile }, None) }).boxed());
                                continue;
                            }

                            let file_path = path.display().to_string();

                            in_stack.push(filename.clone());

                            let filename = format!("/{CHAT_DIRECTORY}/{conversation_id}/{filename}");

                            let mut progress = match constellation.put(&filename, &file_path).await {
                                Ok(stream) => stream,
                                Err(e) => {
                                   tracing::error!(%conversation_id, "Error uploading {filename}: {e}");
                                    streams.insert(kind, stream::once(async { (Progression::ProgressFailed { name: filename, last_size: None, error: e }, None) }).boxed());
                                    continue;
                                }
                            };


                            let directory = root_directory.clone();
                            let filename = filename.to_string();

                            let stream = async_stream::stream! {
                                while let Some(item) = progress.next().await {
                                    match item {
                                        item @ Progression::CurrentProgress { .. } => {
                                            yield (item, None);
                                        },
                                        item @ Progression::ProgressComplete { .. } => {
                                            let file_name = directory.get_item_by_path(&filename).and_then(|item| item.get_file()).ok();
                                            yield (item, file_name);
                                            break;
                                        },
                                        item @ Progression::ProgressFailed { .. } => {
                                            yield (item, None);
                                            break;
                                        }
                                    }
                                }
                            };

                            streams.insert(kind, stream.boxed());
                        }
                    }
                };
            }

            for await (location, (progress, file)) in streams {
                yield AttachmentKind::AttachedProgress(location, progress);
                if let Some(file) = file {
                    attachments.push(file);
                }
            }

            let final_results = {
                async move {

                    if attachments.is_empty() {
                        return Err(Error::NoAttachments);
                    }

                    let mut message = warp::raygun::Message::default();
                    message.set_id(message_id);
                    message.set_message_type(MessageType::Attachment);
                    message.set_conversation_id(conversation_id);
                    message.set_sender(own_did);
                    message.set_attachment(attachments);
                    message.set_lines(messages.clone());
                    message.set_replied(reply_id);

                    let message =
                        MessageDocument::new(&ipfs, &keypair, message, keystore.as_ref()).await?;

                    let (tx, rx) = oneshot::channel();
                    _ = atx.send((message, tx)).await;

                    rx.await.expect("shouldnt drop")
                }
            };

            yield AttachmentKind::Pending(final_results.await)
        };

        Ok((message_id, stream.boxed()))
    }

    async fn store_direct_for_attachment(&mut self, message: MessageDocument) -> Result<(), Error> {
        let conversation_id = self.conversation_id;
        let message_id = message.id;

        let message_cid = self
            .document
            .insert_message_document(&self.ipfs, &message)
            .await?;

        let recipients = self.document.recipients();

        self.set_document().await?;

        let event = MessageEventKind::MessageSent {
            conversation_id,
            message_id,
        };

        if let Err(e) = self.event_broadcast.send(event) {
            tracing::error!(%conversation_id, error = %e, "Error broadcasting event");
        }

        let event = MessagingEvents::New { message };

        if !recipients.is_empty() {
            if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
                for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
                    let _ = self
                        .message_command
                        .clone()
                        .send(MessageCommand::InsertMessage {
                            peer_id,
                            conversation_id,
                            recipients: recipients.clone(),
                            message_id,
                            message_cid,
                        })
                        .await;
                }
            }
        }

        self.publish(Some(message_id), event, true).await
    }

    pub async fn download(
        &self,
        message_id: Uuid,
        file: &str,
        path: PathBuf,
    ) -> Result<ConstellationProgressStream, Error> {
        let members = self
            .document
            .recipients()
            .iter()
            .filter_map(|did| did.to_peer_id().ok())
            .collect::<Vec<_>>();

        let message = self
            .document
            .get_message_document(&self.ipfs, message_id)
            .await?;

        if message.message_type != MessageType::Attachment {
            return Err(Error::InvalidMessage);
        }

        let attachment = message
            .attachments()
            .iter()
            .find(|attachment| attachment.name == file)
            .ok_or(Error::FileNotFound)?;

        let stream = attachment.download(&self.ipfs, path, &members, None);

        Ok(stream)
    }

    pub async fn download_stream(
        &self,
        message_id: Uuid,
        file: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, std::io::Error>>, Error> {
        let members = self
            .document
            .recipients()
            .iter()
            .filter_map(|did| did.to_peer_id().ok())
            .collect::<Vec<_>>();

        let message = self
            .document
            .get_message_document(&self.ipfs, message_id)
            .await?;

        if message.message_type != MessageType::Attachment {
            return Err(Error::InvalidMessage);
        }

        let attachment = message
            .attachments()
            .iter()
            .find(|attachment| attachment.name == file)
            .ok_or(Error::FileNotFound)?;

        let stream = attachment.download_stream(&self.ipfs, &members, None);

        Ok(stream)
    }

    pub async fn publish(
        &mut self,
        message_id: Option<Uuid>,
        event: MessagingEvents,
        queue: bool,
    ) -> Result<(), Error> {
        let event = serde_json::to_vec(&event)?;
        let keypair = self.root.keypair();
        let own_did = self.identity.did_key();

        let key = self.conversation_key(None)?;

        let bytes = Cipher::direct_encrypt(&event, &key)?;

        let payload = PayloadBuilder::new(keypair, bytes)
            .from_ipfs(&self.ipfs)
            .await?;

        let peers = self.ipfs.pubsub_peers(Some(self.document.topic())).await?;

        let mut can_publish = false;

        for recipient in self
            .document
            .recipients()
            .iter()
            .filter(|did| own_did.ne(did))
        {
            let peer_id = recipient.to_peer_id()?;

            // We want to confirm that there is atleast one peer subscribed before attempting to send a message
            match peers.contains(&peer_id) {
                true => {
                    can_publish = true;
                }
                false => {
                    if queue {
                        self.queue_event(
                            recipient.clone(),
                            QueueItem::direct(
                                message_id,
                                peer_id,
                                self.document.topic(),
                                payload.message().to_vec(),
                            ),
                        )
                        .await;
                    }
                }
            };
        }

        if can_publish {
            let bytes = payload.to_bytes()?;
            tracing::trace!(id = %self.conversation_id, "Payload size: {} bytes", bytes.len());
            let timer = Instant::now();
            let mut time = true;
            if let Err(_e) = self.ipfs.pubsub_publish(self.document.topic(), bytes).await {
                tracing::error!(id = %self.conversation_id, "Error publishing: {_e}");
                time = false;
            }
            if time {
                let end = timer.elapsed();
                tracing::trace!(id = %self.conversation_id, "Took {}ms to send event", end.as_millis());
            }
        }

        Ok(())
    }

    async fn queue_event(&mut self, did: DID, queue: QueueItem) {
        self.queue.entry(did).or_default().push(queue);
        self.save_queue().await
    }

    async fn save_queue(&self) {
        let key = format!("{}/{}", self.ipfs.messaging_queue(), self.conversation_id);
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
                _ = self.ipfs.remove_pin(old_cid).recursive().await;
            }
        }
    }

    async fn add_exclusion(&mut self, member: DID, signature: String) -> Result<(), Error> {
        let conversation_id = self.conversation_id;
        if !matches!(self.document.conversation_type(), ConversationType::Group) {
            return Err(anyhow::anyhow!("Can only leave from a group conversation").into());
        }

        let Some(creator) = self.document.creator.as_ref() else {
            return Err(anyhow::anyhow!("Group conversation requires a creator").into());
        };

        let own_did = self.identity.did_key();

        // Precaution
        if member.eq(creator) {
            return Err(anyhow::anyhow!("Cannot remove the creator of the group").into());
        }

        if !self.document.recipients.contains(&member) {
            return Err(anyhow::anyhow!("{member} does not belong to {conversation_id}").into());
        }

        tracing::info!("{member} is leaving group conversation {conversation_id}");

        if creator.eq(&own_did) {
            self.remove_participant(&member, false).await?;
        } else {
            {
                //Small validation context
                let context = format!("exclude {}", member);
                let signature = bs58::decode(&signature).into_vec()?;
                verify_serde_sig(member.clone(), &context, &signature)?;
            }

            //Validate again since we have a permit
            if !self.document.recipients.contains(&member) {
                return Err(
                    anyhow::anyhow!("{member} does not belong to {conversation_id}").into(),
                );
            }

            let mut can_emit = false;

            if let Entry::Vacant(entry) = self.document.excluded.entry(member.clone()) {
                entry.insert(signature);
                can_emit = true;
            }
            self.set_document().await?;
            if can_emit {
                if let Err(e) = self
                    .event_broadcast
                    .send(MessageEventKind::RecipientRemoved {
                        conversation_id,
                        recipient: member,
                    })
                {
                    tracing::error!("Error broadcasting event: {e}");
                }
            }
        }
        Ok(())
    }
}

async fn message_event(
    this: &mut ConversationTask,
    sender: &DID,
    events: MessagingEvents,
) -> Result<(), Error> {
    let conversation_id = this.conversation_id;

    let keypair = this.root.keypair();
    let own_did = this.identity.did_key();

    let keystore = pubkey_or_keystore(&*this)?;

    match events {
        MessagingEvents::New { message } => {
            if !message.verify() {
                return Err(Error::InvalidMessage);
            }

            if this.document.id != message.conversation_id {
                return Err(Error::InvalidConversation);
            }

            let message_id = message.id;

            if !this
                .document
                .recipients()
                .contains(&message.sender.to_did())
            {
                return Err(Error::IdentityDoesntExist);
            }

            if this.document.contains(&this.ipfs, message_id).await? {
                return Err(Error::MessageFound);
            }

            let resolved_message = message
                .resolve(&this.ipfs, keypair, false, keystore.as_ref())
                .await?;

            let lines_value_length: usize = resolved_message
                .lines()
                .iter()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.chars().count())
                .sum();

            if lines_value_length == 0 && lines_value_length > MAX_MESSAGE_SIZE {
                tracing::error!(
                    message_length = lines_value_length,
                    "Length of message is invalid."
                );
                return Err(Error::InvalidLength {
                    context: "message".into(),
                    current: lines_value_length,
                    minimum: Some(MIN_MESSAGE_SIZE),
                    maximum: Some(MAX_MESSAGE_SIZE),
                });
            }

            let conversation_id = message.conversation_id;

            this.document
                .insert_message_document(&this.ipfs, &message)
                .await?;

            this.set_document().await?;

            if let Err(e) = this
                .event_broadcast
                .send(MessageEventKind::MessageReceived {
                    conversation_id,
                    message_id,
                })
            {
                tracing::warn!(%conversation_id, "Error broadcasting event: {e}");
            }
        }
        MessagingEvents::Edit {
            conversation_id,
            message_id,
            modified,
            lines,
            nonce,
            signature,
        } => {
            let mut message_document = this
                .document
                .get_message_document(&this.ipfs, message_id)
                .await?;

            let mut message = message_document
                .resolve(&this.ipfs, keypair, true, keystore.as_ref())
                .await?;

            let lines_value_length: usize = lines
                .iter()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.chars().count())
                .sum();

            if lines_value_length == 0 && lines_value_length > MAX_MESSAGE_SIZE {
                tracing::error!(
                    current_size = lines_value_length,
                    max = MAX_MESSAGE_SIZE,
                    "length of message is invalid"
                );
                return Err(Error::InvalidLength {
                    context: "message".into(),
                    current: lines_value_length,
                    minimum: Some(MIN_MESSAGE_SIZE),
                    maximum: Some(MAX_MESSAGE_SIZE),
                });
            }

            let sender = message.sender();

            *message.lines_mut() = lines;
            message.set_modified(modified);

            message_document
                .update(
                    &this.ipfs,
                    keypair,
                    message,
                    (!signature.is_empty() && sender.ne(&own_did)).then_some(signature),
                    keystore.as_ref(),
                    Some(nonce.as_slice()),
                )
                .await?;

            this.document
                .update_message_document(&this.ipfs, &message_document)
                .await?;

            this.set_document().await?;

            if let Err(e) = this.event_broadcast.send(MessageEventKind::MessageEdited {
                conversation_id,
                message_id,
            }) {
                tracing::error!(%conversation_id, error = %e, "Error broadcasting event");
            }
        }
        MessagingEvents::Delete {
            conversation_id,
            message_id,
        } => {
            // if opt.keep_if_owned.load(Ordering::SeqCst) {
            //     let message_document = document
            //         .get_message_document(&self.ipfs, message_id)
            //         .await?;

            //     let message = message_document
            //         .resolve(&self.ipfs, &self.keypair, true, keystore.as_ref())
            //         .await?;

            //     if message.sender() == *self.keypair {
            //         return Ok(());
            //     }
            // }

            this.document.delete_message(&this.ipfs, message_id).await?;

            this.set_document().await?;

            if let Err(e) = this.event_broadcast.send(MessageEventKind::MessageDeleted {
                conversation_id,
                message_id,
            }) {
                tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
            }
        }
        MessagingEvents::Pin {
            conversation_id,
            message_id,
            state,
            ..
        } => {
            let mut message_document = this
                .document
                .get_message_document(&this.ipfs, message_id)
                .await?;

            let mut message = message_document
                .resolve(&this.ipfs, keypair, true, keystore.as_ref())
                .await?;

            let event = match state {
                PinState::Pin => {
                    if message.pinned() {
                        return Ok(());
                    }
                    *message.pinned_mut() = true;
                    MessageEventKind::MessagePinned {
                        conversation_id,
                        message_id,
                    }
                }
                PinState::Unpin => {
                    if !message.pinned() {
                        return Ok(());
                    }
                    *message.pinned_mut() = false;
                    MessageEventKind::MessageUnpinned {
                        conversation_id,
                        message_id,
                    }
                }
            };

            message_document
                .update(&this.ipfs, keypair, message, None, keystore.as_ref(), None)
                .await?;

            this.document
                .update_message_document(&this.ipfs, &message_document)
                .await?;

            this.set_document().await?;

            if let Err(e) = this.event_broadcast.send(event) {
                tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
            }
        }
        MessagingEvents::React {
            conversation_id,
            reactor,
            message_id,
            state,
            emoji,
        } => {
            let mut message_document = this
                .document
                .get_message_document(&this.ipfs, message_id)
                .await?;

            let mut message = message_document
                .resolve(&this.ipfs, keypair, true, keystore.as_ref())
                .await?;

            let reactions = message.reactions_mut();

            match state {
                ReactionState::Add => {
                    if reactions.len() >= MAX_REACTIONS {
                        return Err(Error::InvalidLength {
                            context: "reactions".into(),
                            current: reactions.len(),
                            minimum: None,
                            maximum: Some(MAX_REACTIONS),
                        });
                    }

                    let entry = reactions.entry(emoji.clone()).or_default();

                    if entry.contains(&reactor) {
                        return Err(Error::ReactionExist);
                    }

                    entry.push(reactor.clone());

                    message_document
                        .update(&this.ipfs, keypair, message, None, keystore.as_ref(), None)
                        .await?;

                    this.document
                        .update_message_document(&this.ipfs, &message_document)
                        .await?;

                    this.set_document().await?;

                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::MessageReactionAdded {
                                conversation_id,
                                message_id,
                                did_key: reactor,
                                reaction: emoji,
                            })
                    {
                        tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                    }
                }
                ReactionState::Remove => {
                    match reactions.entry(emoji.clone()) {
                        indexmap::map::Entry::Occupied(mut e) => {
                            let list = e.get_mut();

                            if !list.contains(&reactor) {
                                return Err(Error::ReactionDoesntExist);
                            }

                            list.retain(|did| did != &reactor);
                            if list.is_empty() {
                                e.swap_remove();
                            }
                        }
                        indexmap::map::Entry::Vacant(_) => return Err(Error::ReactionDoesntExist),
                    };

                    message_document
                        .update(&this.ipfs, keypair, message, None, keystore.as_ref(), None)
                        .await?;

                    this.document
                        .update_message_document(&this.ipfs, &message_document)
                        .await?;

                    this.set_document().await?;

                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::MessageReactionRemoved {
                                conversation_id,
                                message_id,
                                did_key: reactor,
                                reaction: emoji,
                            })
                    {
                        tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                    }
                }
            }
        }
        MessagingEvents::UpdateConversation {
            mut conversation,
            kind,
        } => {
            conversation.verify()?;
            conversation.excluded = this.document.excluded.clone();
            conversation.messages = this.document.messages;
            conversation.favorite = this.document.favorite;
            conversation.archived = this.document.archived;

            match kind {
                ConversationUpdateKind::AddParticipant { did } => {
                    if !this.document.creator.as_ref().is_some_and(|c| c == sender)
                        && !this
                            .document
                            .permissions
                            .has_permission(sender, GroupPermission::AddParticipants)
                    {
                        return Err(Error::Unauthorized);
                    }

                    if this.document.recipients.contains(&did) {
                        return Ok(());
                    }

                    if !this.discovery.contains(&did).await {
                        let _ = this.discovery.insert(&did).await;
                    }

                    this.replace_document(conversation).await?;

                    if let Err(e) = this.request_key(&did).await {
                        tracing::error!(%conversation_id, error = %e, "error requesting key");
                    }

                    if let Err(e) = this.event_broadcast.send(MessageEventKind::RecipientAdded {
                        conversation_id,
                        recipient: did,
                    }) {
                        tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                    }
                }
                ConversationUpdateKind::RemoveParticipant { did } => {
                    if !this.document.creator.as_ref().is_some_and(|c| c == sender) {
                        return Err(Error::Unauthorized);
                    }
                    if !this.document.recipients.contains(&did) {
                        return Err(Error::IdentityDoesntExist);
                    }

                    this.document.permissions.shift_remove(&did);

                    //Maybe remove participant from discovery?

                    let can_emit = !this.document.excluded.contains_key(&did);

                    this.document.excluded.remove(&did);

                    this.replace_document(conversation).await?;

                    if can_emit {
                        if let Err(e) =
                            this.event_broadcast
                                .send(MessageEventKind::RecipientRemoved {
                                    conversation_id,
                                    recipient: did,
                                })
                        {
                            tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                        }
                    }
                }
                ConversationUpdateKind::ChangeName { name: Some(name) } => {
                    if !this.document.creator.as_ref().is_some_and(|c| c == sender)
                        && !this
                            .document
                            .permissions
                            .has_permission(sender, GroupPermission::SetGroupName)
                    {
                        return Err(Error::Unauthorized);
                    }

                    let name = name.trim();
                    let name_length = name.len();

                    if name_length > 255 {
                        return Err(Error::InvalidLength {
                            context: "name".into(),
                            current: name_length,
                            minimum: None,
                            maximum: Some(255),
                        });
                    }
                    if let Some(current_name) = this.document.name.as_ref() {
                        if current_name.eq(&name) {
                            return Ok(());
                        }
                    }

                    this.replace_document(conversation).await?;

                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::ConversationNameUpdated {
                                conversation_id,
                                name: name.to_string(),
                            })
                    {
                        tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                    }
                }

                ConversationUpdateKind::ChangeName { name: None } => {
                    if !this.document.creator.as_ref().is_some_and(|c| c == sender)
                        && !this
                            .document
                            .permissions
                            .has_permission(sender, GroupPermission::SetGroupName)
                    {
                        return Err(Error::Unauthorized);
                    }

                    this.replace_document(conversation).await?;

                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::ConversationNameUpdated {
                                conversation_id,
                                name: String::new(),
                            })
                    {
                        tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                    }
                }
                ConversationUpdateKind::AddRestricted { .. }
                | ConversationUpdateKind::RemoveRestricted { .. } => {
                    if !this.document.creator.as_ref().is_some_and(|c| c == sender) {
                        return Err(Error::Unauthorized);
                    }
                    this.replace_document(conversation).await?;
                    //TODO: Maybe add a api event to emit for when blocked users are added/removed from the document
                    //      but for now, we can leave this as a silent update since the block list would be for internal handling for now
                }
                ConversationUpdateKind::ChangePermissions { permissions } => {
                    if !this.document.creator.as_ref().is_some_and(|c| c == sender) {
                        return Err(Error::Unauthorized);
                    }

                    let (added, removed) = this.document.permissions.compare_with_new(&permissions);
                    this.document.permissions = permissions;
                    this.replace_document(conversation).await?;

                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::ConversationPermissionsUpdated {
                            conversation_id,
                            added,
                            removed,
                        },
                    ) {
                        tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                    }
                }
                ConversationUpdateKind::AddedIcon | ConversationUpdateKind::RemovedIcon => {
                    this.replace_document(conversation).await?;

                    if let Err(e) = this
                        .event_broadcast
                        .send(MessageEventKind::ConversationUpdatedIcon { conversation_id })
                    {
                        tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                    }
                }

                ConversationUpdateKind::AddedBanner | ConversationUpdateKind::RemovedBanner => {
                    this.replace_document(conversation).await?;

                    if let Err(e) = this
                        .event_broadcast
                        .send(MessageEventKind::ConversationUpdatedBanner { conversation_id })
                    {
                        tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                    }
                }
                ConversationUpdateKind::ChangeDescription { description } => {
                    if let Some(desc) = description.as_ref() {
                        if desc.is_empty() || desc.len() > MAX_CONVERSATION_DESCRIPTION {
                            return Err(Error::InvalidLength {
                                context: "description".into(),
                                minimum: Some(1),
                                maximum: Some(MAX_CONVERSATION_DESCRIPTION),
                                current: desc.len(),
                            });
                        }

                        if matches!(this.document.description.as_ref(), Some(current_desc) if current_desc == desc)
                        {
                            return Ok(());
                        }
                    }

                    this.replace_document(conversation).await?;
                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::ConversationDescriptionChanged {
                            conversation_id,
                            description,
                        },
                    ) {
                        tracing::warn!(%conversation_id, error = %e, "Error broadcasting event");
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

async fn process_request_response_event(
    this: &mut ConversationTask,
    req: Message,
) -> Result<(), Error> {
    let keypair = &this.root.keypair().clone();
    let own_did = this.identity.did_key();

    let payload = PayloadMessage::<Vec<u8>>::from_bytes(&req.data)?;

    let sender = payload.sender().to_did()?;

    let data = ecdh_decrypt(keypair, Some(&sender), payload.message())?;

    let event = serde_json::from_slice::<ConversationRequestResponse>(&data)?;

    tracing::debug!(id=%this.conversation_id, ?event, "Event received");
    match event {
        ConversationRequestResponse::Request {
            conversation_id,
            kind,
        } => match kind {
            ConversationRequestKind::Key => {
                if !matches!(this.document.conversation_type(), ConversationType::Group) {
                    //Only group conversations support keys
                    return Err(Error::InvalidConversation);
                }

                if !this.document.recipients().contains(&sender) {
                    tracing::warn!(%conversation_id, %sender, "apart of conversation");
                    return Err(Error::IdentityDoesntExist);
                }

                let keystore = &mut this.keystore;

                let raw_key = match keystore.get_latest(keypair, &own_did) {
                    Ok(key) => key,
                    Err(Error::PublicKeyDoesntExist) => {
                        let key = generate::<64>().into();
                        keystore.insert(keypair, &own_did, &key)?;

                        this.set_keystore().await?;
                        key
                    }
                    Err(e) => {
                        tracing::error!(%conversation_id, error = %e, "Error getting key from store");
                        return Err(e);
                    }
                };

                let key = ecdh_encrypt(keypair, Some(&sender), raw_key)?;

                let response = ConversationRequestResponse::Response {
                    conversation_id,
                    kind: ConversationResponseKind::Key { key },
                };

                let topic = this.document.exchange_topic(&sender);

                let bytes = ecdh_encrypt(keypair, Some(&sender), serde_json::to_vec(&response)?)?;

                let payload = PayloadBuilder::new(keypair, bytes)
                    .from_ipfs(&this.ipfs)
                    .await?;

                let peers = this.ipfs.pubsub_peers(Some(topic.clone())).await?;

                let peer_id = sender.to_peer_id()?;

                let bytes = payload.to_bytes()?;

                tracing::trace!(%conversation_id, "Payload size: {} bytes", bytes.len());

                tracing::info!(%conversation_id, "Responding to {sender}");

                if !peers.contains(&peer_id)
                    || (peers.contains(&peer_id)
                        && this
                            .ipfs
                            .pubsub_publish(topic.clone(), bytes)
                            .await
                            .is_err())
                {
                    tracing::warn!(%conversation_id, "Unable to publish to topic. Queuing event");
                    // TODO
                    this.queue_event(
                        sender.clone(),
                        QueueItem::direct(None, peer_id, topic.clone(), payload.message().to_vec()),
                    )
                    .await;
                }
            }
            ConversationRequestKind::Ping => {
                let response = ConversationRequestResponse::Response {
                    conversation_id,
                    kind: ConversationResponseKind::Pong,
                };

                let topic = this.document.exchange_topic(&sender);

                let bytes = ecdh_encrypt(keypair, Some(&sender), serde_json::to_vec(&response)?)?;

                let payload = PayloadBuilder::new(keypair, bytes)
                    .from_ipfs(&this.ipfs)
                    .await?;

                let bytes = payload.to_bytes()?;

                tracing::trace!(%conversation_id, "Payload size: {} bytes", bytes.len());

                tracing::info!(%conversation_id, "Responding to {sender}");

                let _ = this.ipfs.pubsub_publish(topic, bytes).await;
            }
            _ => {
                tracing::info!(%conversation_id, "Unimplemented/Unsupported Event");
            }
        },
        ConversationRequestResponse::Response {
            conversation_id,
            kind,
        } => match kind {
            ConversationResponseKind::Key { key } => {
                if !matches!(this.document.conversation_type(), ConversationType::Group) {
                    //Only group conversations support keys
                    tracing::error!(%conversation_id, "Invalid conversation type");
                    return Err(Error::InvalidConversation);
                }

                if !this.document.recipients().contains(&sender) {
                    return Err(Error::IdentityDoesntExist);
                }
                let keystore = &mut this.keystore;

                let raw_key = ecdh_decrypt(keypair, Some(&sender), key)?;

                keystore.insert(keypair, &sender, raw_key)?;

                this.set_keystore().await?;

                if let Some((_, received)) = this.pending_key_exchange.get_mut(&sender) {
                    *received = true;
                }
            }
            ConversationResponseKind::Pong => {
                if this.pending_ping_response.remove(&sender).is_none() {
                    // Note: Never sent a ping request so we can reject it
                    // TODO: Possibly blacklist peer if a request was never sent, however, we have to consider
                    //       the possibility of the peer reinitializing the task (ie, restarting) after the ping request is sent out
                    //       and possibly receiving a response after.
                    return Ok(());
                }
                if let Some(instant) = this.ping_duration.shift_remove(&sender) {
                    // Note: The response time should be taken with a grain of salt due to the stream of messages from gossipsub and how messages
                    //       may be queued. Therefore, is it best to use this as an approx response time and not explicit.
                    // TODO: Maybe rely on a connection stream instead for peers within a conversation for pinging
                    let end = instant.elapsed();
                    tracing::info!(conversation_id=%conversation_id, %sender, "took {}ms to response", end.as_millis());
                }

                // Perform a check to determine if we have a key for the user. If not, request it
                if matches!(this.document.conversation_type(), ConversationType::Direct) {
                    return Ok(());
                }

                // TODO: Maybe ignore the recipients list when sending to a common topic
                if !this.document.recipients().contains(&sender) {
                    return Err(Error::IdentityDoesntExist);
                }

                if this.keystore.exist(&sender) {
                    return Ok(());
                }

                _ = this.request_key(&sender).await
            }
            _ => {
                tracing::info!(%conversation_id, "Unimplemented/Unsupported Event");
            }
        },
    }
    Ok(())
}

async fn process_pending_payload(this: &mut ConversationTask) {
    let _this = this.borrow_mut();
    let conversation_id = _this.conversation_id;
    if _this.pending_key_exchange.is_empty() {
        return;
    }

    let root = _this.root.clone();

    let mut processed_events: IndexSet<_> = IndexSet::new();

    _this.pending_key_exchange.retain(|did, (data, received)| {
        if *received {
            processed_events.insert((did.clone(), data.clone()));
            return false;
        }
        true
    });

    let store = _this.keystore.clone();

    for (sender, data) in processed_events {
        // Note: Conversation keystore should exist so we could expect here, however since the map for pending exchanges would have
        //       been flushed out, we can just continue on in the iteration since it would be ignored

        let event_fn = || {
            let keypair = root.keypair();
            let key = store.get_latest(keypair, &sender)?;
            let data = Cipher::direct_decrypt(&data, &key)?;
            let event = serde_json::from_slice(&data)?;
            Ok::<_, Error>(event)
        };

        let event = match event_fn() {
            Ok(event) => event,
            Err(e) => {
                tracing::error!(name = "process_pending_payload", %conversation_id, %sender, error = %e, "failed to process message");
                continue;
            }
        };

        if let Err(e) = message_event(this, &sender, event).await {
            tracing::error!(name = "process_pending_payload", %conversation_id, %sender, error = %e, "failed to process message")
        }
    }
}

async fn process_conversation_event(
    this: &mut ConversationTask,
    message: Message,
) -> Result<(), Error> {
    let payload = PayloadMessage::<Vec<u8>>::from_bytes(&message.data)?;
    let sender = payload.sender().to_did()?;

    let key = this.conversation_key(Some(&sender))?;

    let data = Cipher::direct_decrypt(payload.message(), &key)?;

    let event = match serde_json::from_slice::<MessagingEvents>(&data)? {
        event @ MessagingEvents::Event { .. } => event,
        _ => return Err(Error::Other),
    };

    if let MessagingEvents::Event {
        conversation_id,
        member,
        event,
        cancelled,
    } = event
    {
        let ev = match cancelled {
            true => MessageEventKind::EventCancelled {
                conversation_id,
                did_key: member,
                event,
            },
            false => MessageEventKind::EventReceived {
                conversation_id,
                did_key: member,
                event,
            },
        };

        if let Err(e) = this.event_broadcast.send(ev) {
            tracing::error!(%conversation_id, error = %e, "error broadcasting event");
        }
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
struct QueueItem {
    m_id: Option<Uuid>,
    peer: PeerId,
    topic: String,
    data: Vec<u8>,
    sent: bool,
}

impl QueueItem {
    pub fn direct(m_id: Option<Uuid>, peer: PeerId, topic: String, data: Vec<u8>) -> Self {
        QueueItem {
            m_id,
            peer,
            topic,
            data,
            sent: false,
        }
    }
}

//TODO: Replace
async fn process_queue(this: &mut ConversationTask) {
    let mut changed = false;
    let keypair = &this.root.keypair().clone();
    for (did, items) in this.queue.iter_mut() {
        let Ok(peer_id) = did.to_peer_id() else {
            continue;
        };

        if !this.ipfs.is_connected(peer_id).await.unwrap_or_default() {
            continue;
        }

        // TODO:
        for item in items {
            let QueueItem {
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

            let payload = match PayloadBuilder::<_>::new(keypair, data.clone())
                .from_ipfs(&this.ipfs)
                .await
            {
                Ok(p) => p,
                Err(_e) => {
                    // tracing::warn!(error = %_e, "unable to build payload")
                    continue;
                }
            };

            let Ok(bytes) = payload.to_bytes() else {
                continue;
            };

            if let Err(e) = this.ipfs.pubsub_publish(topic.clone(), bytes).await {
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

fn pubkey_or_keystore(conversation: &ConversationTask) -> Result<Either<DID, Keystore>, Error> {
    let keypair = conversation.root.keypair();
    let keystore = match conversation.document.conversation_type() {
        ConversationType::Direct => {
            let list = conversation.document.recipients();

            let own_did = keypair.to_did()?;

            let recipients = list
                .into_iter()
                .filter(|did| own_did.ne(did))
                .collect::<Vec<_>>();

            let member = recipients
                .first()
                .cloned()
                .ok_or(Error::InvalidConversation)?;

            Either::Left(member)
        }
        ConversationType::Group => Either::Right(conversation.keystore.clone()),
    };

    Ok(keystore)
}
