use bytes::Bytes;
use chrono::{DateTime, Utc};
use either::Either;
use futures::channel::oneshot;
use futures::stream::BoxStream;
use futures::{StreamExt, TryFutureExt};
use futures_timer::Delay;
use indexmap::{IndexMap, IndexSet};
use ipld_core::cid::Cid;
use rust_ipfs::libp2p::gossipsub::Message;
use rust_ipfs::{Ipfs, IpfsPath};
use rust_ipfs::{PeerId, SubscriptionStream};
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use uuid::Uuid;
use warp::constellation::ConstellationProgressStream;
use warp::crypto::DID;
use warp::raygun::community::{
    CommunityChannel, CommunityChannelPermission, CommunityChannelType, CommunityInvite,
    CommunityPermission, CommunityRole, RoleId,
};
use warp::raygun::{
    AttachmentEventStream, ConversationImage, Location, MessageEvent, MessageOptions,
    MessageReference, MessageStatus, MessageType, Messages, MessagesType, PinState,
    RayGunEventKind, ReactionState,
};
use warp::{crypto::generate, error::Error, raygun::MessageEventKind};
use web_time::Instant;

use crate::store::community::{
    CommunityChannelDocument, CommunityDocument, CommunityInviteDocument, CommunityRoleDocument,
};
use crate::store::conversation::message::{MessageDocument, MessageDocumentBuilder};
use crate::store::discovery::Discovery;
use crate::store::document::files::FileDocument;
use crate::store::document::image_dag::ImageDag;
use crate::store::ds_key::DataStoreKey;
use crate::store::event_subscription::EventSubscription;
use crate::store::topics::PeerTopic;
use crate::store::{
    CommunityJoinEvents, CommunityUpdateKind, ConversationEvents, ConversationImageType,
    MAX_COMMUNITY_CHANNELS, MAX_COMMUNITY_DESCRIPTION, MAX_CONVERSATION_BANNER_SIZE,
    MAX_CONVERSATION_ICON_SIZE, MAX_MESSAGE_SIZE, MIN_MESSAGE_SIZE,
};
use crate::utils::{ByteCollection, ExtensionType};
use crate::{
    // rt::LocalExecutor,
    store::{
        document::root::RootDocumentMap,
        ecdh_decrypt, ecdh_encrypt,
        files::FileStore,
        identity::IdentityStore,
        keystore::Keystore,
        payload::{PayloadBuilder, PayloadMessage},
        CommunityMessagingEvents, ConversationRequestKind, ConversationRequestResponse,
        ConversationResponseKind, DidExt, PeerIdExt,
    },
};

use super::attachment::AttachmentStream;

type AttachmentOneshot = (MessageDocument, oneshot::Sender<Result<(), Error>>);

#[allow(dead_code)]
#[derive(Debug)]
pub enum CommunityTaskCommand {
    LeaveCommunity {
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetCommunityIcon {
        response: oneshot::Sender<Result<ConversationImage, Error>>,
    },
    GetCommunityBanner {
        response: oneshot::Sender<Result<ConversationImage, Error>>,
    },
    EditCommunityIcon {
        location: Location,
        response: oneshot::Sender<Result<(), Error>>,
    },
    EditCommunityBanner {
        location: Location,
        response: oneshot::Sender<Result<(), Error>>,
    },
    CreateCommunityInvite {
        target_user: Option<DID>,
        expiry: Option<DateTime<Utc>>,
        response: oneshot::Sender<Result<CommunityInvite, Error>>,
    },
    DeleteCommunityInvite {
        invite_id: Uuid,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetCommunityInvite {
        invite_id: Uuid,
        response: oneshot::Sender<Result<CommunityInvite, Error>>,
    },
    EditCommunityInvite {
        invite_id: Uuid,
        invite: CommunityInvite,
        response: oneshot::Sender<Result<(), Error>>,
    },
    CreateCommunityRole {
        name: String,
        response: oneshot::Sender<Result<CommunityRole, Error>>,
    },
    DeleteCommunityRole {
        role_id: RoleId,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetCommunityRole {
        role_id: RoleId,
        response: oneshot::Sender<Result<CommunityRole, Error>>,
    },
    EditCommunityRoleName {
        role_id: RoleId,
        new_name: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GrantCommunityRole {
        role_id: RoleId,
        user: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RevokeCommunityRole {
        role_id: RoleId,
        user: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    CreateCommunityChannel {
        channel_name: String,
        channel_type: CommunityChannelType,
        response: oneshot::Sender<Result<CommunityChannel, Error>>,
    },
    DeleteCommunityChannel {
        channel_id: Uuid,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GetCommunityChannel {
        channel_id: Uuid,
        response: oneshot::Sender<Result<CommunityChannel, Error>>,
    },
    EditCommunityName {
        name: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    EditCommunityDescription {
        description: Option<String>,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GrantCommunityPermission {
        permission: String,
        role_id: RoleId,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RevokeCommunityPermission {
        permission: String,
        role_id: RoleId,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GrantCommunityPermissionForAll {
        permission: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RevokeCommunityPermissionForAll {
        permission: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    HasCommunityPermission {
        permission: String,
        member: DID,
        response: oneshot::Sender<Result<bool, Error>>,
    },
    RemoveCommunityMember {
        member: DID,
        response: oneshot::Sender<Result<(), Error>>,
    },
    EditCommunityChannelName {
        channel_id: Uuid,
        name: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    EditCommunityChannelDescription {
        channel_id: Uuid,
        description: Option<String>,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GrantCommunityChannelPermission {
        channel_id: Uuid,
        permission: String,
        role_id: RoleId,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RevokeCommunityChannelPermission {
        channel_id: Uuid,
        permission: String,
        role_id: RoleId,
        response: oneshot::Sender<Result<(), Error>>,
    },
    GrantCommunityChannelPermissionForAll {
        channel_id: Uuid,
        permission: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RevokeCommunityChannelPermissionForAll {
        channel_id: Uuid,
        permission: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    HasCommunityChannelPermission {
        channel_id: Uuid,
        permission: String,
        member: DID,
        response: oneshot::Sender<Result<bool, Error>>,
    },
    GetCommunityChannelMessage {
        channel_id: Uuid,
        message_id: Uuid,
        response: oneshot::Sender<Result<warp::raygun::Message, Error>>,
    },
    GetCommunityChannelMessages {
        channel_id: Uuid,
        options: MessageOptions,
        response: oneshot::Sender<Result<Messages, Error>>,
    },
    GetCommunityChannelMessageCount {
        channel_id: Uuid,
        response: oneshot::Sender<Result<usize, Error>>,
    },
    GetCommunityChannelMessageReference {
        channel_id: Uuid,
        message_id: Uuid,
        response: oneshot::Sender<Result<MessageReference, Error>>,
    },
    GetCommunityChannelMessageReferences {
        channel_id: Uuid,
        options: MessageOptions,
        response: oneshot::Sender<Result<BoxStream<'static, MessageReference>, Error>>,
    },
    CommunityChannelMessageStatus {
        channel_id: Uuid,
        message_id: Uuid,
        response: oneshot::Sender<Result<MessageStatus, Error>>,
    },
    SendCommunityChannelMessage {
        channel_id: Uuid,
        message: Vec<String>,
        response: oneshot::Sender<Result<Uuid, Error>>,
    },
    EditCommunityChannelMessage {
        channel_id: Uuid,
        message_id: Uuid,
        message: Vec<String>,
        response: oneshot::Sender<Result<(), Error>>,
    },
    ReplyToCommunityChannelMessage {
        channel_id: Uuid,
        message_id: Uuid,
        message: Vec<String>,
        response: oneshot::Sender<Result<Uuid, Error>>,
    },
    DeleteCommunityChannelMessage {
        channel_id: Uuid,
        message_id: Uuid,
        response: oneshot::Sender<Result<(), Error>>,
    },
    PinCommunityChannelMessage {
        channel_id: Uuid,
        message_id: Uuid,
        state: PinState,
        response: oneshot::Sender<Result<(), Error>>,
    },
    ReactToCommunityChannelMessage {
        channel_id: Uuid,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
        response: oneshot::Sender<Result<(), Error>>,
    },
    SendCommunityChannelMesssageEvent {
        channel_id: Uuid,
        event: MessageEvent,
        response: oneshot::Sender<Result<(), Error>>,
    },
    CancelCommunityChannelMesssageEvent {
        channel_id: Uuid,
        event: MessageEvent,
        response: oneshot::Sender<Result<(), Error>>,
    },
    AttachToCommunityChannelMessage {
        channel_id: Uuid,
        message_id: Option<Uuid>,
        locations: Vec<Location>,
        message: Vec<String>,
        response: oneshot::Sender<Result<(Uuid, AttachmentEventStream), Error>>,
    },
    DownloadFromCommunityChannelMessage {
        channel_id: Uuid,
        message_id: Uuid,
        file: String,
        path: PathBuf,
        response: oneshot::Sender<Result<ConstellationProgressStream, Error>>,
    },
    DownloadStreamFromCommunityChannelMessage {
        channel_id: Uuid,
        message_id: Uuid,
        file: String,
        response: oneshot::Sender<Result<BoxStream<'static, Result<Bytes, std::io::Error>>, Error>>,
    },

    SendJoinedCommunityEvent {
        response: oneshot::Sender<Result<(), Error>>,
    },
    EventHandler {
        response: oneshot::Sender<tokio::sync::broadcast::Sender<MessageEventKind>>,
    },
    Delete {
        response: oneshot::Sender<Result<(), Error>>,
    },
}

pub struct CommunityTask {
    community_id: Uuid,
    ipfs: Ipfs,
    root: RootDocumentMap,
    file: FileStore,
    identity: IdentityStore,
    discovery: Discovery,
    pending_key_exchange: IndexMap<DID, Vec<(Bytes, bool)>>,
    document: CommunityDocument,
    keystore: Keystore,

    messaging_stream: SubscriptionStream,
    event_stream: SubscriptionStream,
    request_stream: SubscriptionStream,
    join_stream: SubscriptionStream,

    attachment_tx: futures::channel::mpsc::Sender<AttachmentOneshot>,
    attachment_rx: futures::channel::mpsc::Receiver<AttachmentOneshot>,
    event_broadcast: tokio::sync::broadcast::Sender<MessageEventKind>,
    _event_subscription: EventSubscription<RayGunEventKind>,

    command_rx: futures::channel::mpsc::Receiver<CommunityTaskCommand>,

    //TODO: replace queue
    queue: HashMap<DID, Vec<QueueItem>>,

    terminate: CommunityTermination,
}

#[derive(Default, Debug)]
struct CommunityTermination {
    terminate: bool,
    waker: Option<Waker>,
}

impl CommunityTermination {
    fn cancel(&mut self) {
        self.terminate = true;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl Future for CommunityTermination {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.terminate {
            return Poll::Ready(());
        }

        self.waker.replace(cx.waker().clone());
        Poll::Pending
    }
}

impl CommunityTask {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        community_id: Uuid,
        ipfs: &Ipfs,
        root: &RootDocumentMap,
        identity: &IdentityStore,
        file: &FileStore,
        discovery: &Discovery,
        command_rx: futures::channel::mpsc::Receiver<CommunityTaskCommand>,
        _event_subscription: EventSubscription<RayGunEventKind>,
    ) -> Result<Self, Error> {
        let document = root.get_community_document(community_id).await?;
        let main_topic = document.topic();
        let event_topic = document.event_topic();
        let request_topic = document.exchange_topic(&identity.did_key());
        let join_topic = document.join_topic();

        let messaging_stream = ipfs.pubsub_subscribe(main_topic).await?;
        let event_stream = ipfs.pubsub_subscribe(event_topic).await?;
        let request_stream = ipfs.pubsub_subscribe(request_topic).await?;
        let join_stream = ipfs.pubsub_subscribe(join_topic).await?;

        let (atx, arx) = futures::channel::mpsc::channel(256);
        let (btx, _) = tokio::sync::broadcast::channel(1024);
        let mut task = Self {
            community_id,
            ipfs: ipfs.clone(),
            root: root.clone(),
            file: file.clone(),
            identity: identity.clone(),
            discovery: discovery.clone(),
            pending_key_exchange: Default::default(),
            document,
            keystore: Keystore::default(),

            messaging_stream,
            request_stream,
            event_stream,
            join_stream,

            attachment_tx: atx,
            attachment_rx: arx,
            event_broadcast: btx,
            _event_subscription,
            command_rx,
            queue: Default::default(),
            terminate: CommunityTermination::default(),
        };

        task.keystore = match root.get_keystore(community_id).await {
            Ok(store) => store,
            Err(_) => {
                let mut store = Keystore::new();
                store.insert(root.keypair(), &identity.did_key(), generate::<64>())?;
                task.set_keystore(Some(&store)).await?;
                store
            }
        };

        let key = format!("{}/{}", ipfs.messaging_queue(), community_id);

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

        tracing::info!(%community_id, "community task created");
        Ok(task)
    }
}

impl CommunityTask {
    pub async fn run(mut self) {
        let this = &mut self;

        let community_id = this.community_id;

        let mut queue_timer = Delay::new(Duration::from_secs(1));

        let mut pending_exchange_timer = Delay::new(Duration::from_secs(1));

        let mut check_mailbox = Delay::new(Duration::from_secs(5));

        loop {
            tokio::select! {
                biased;
                _ = &mut this.terminate => {
                    break;
                }
                Some(command) = this.command_rx.next() => {
                    this.process_command(command).await;
                }
                Some((message, response)) = this.attachment_rx.next() => {
                    let _ = response.send(this.store_direct_for_attachment(message).await);
                }
                Some(request) = this.request_stream.next() => {
                    let source = request.source;
                    if let Err(e) = process_request_response_event(this, request).await {
                        tracing::error!(%community_id, sender = ?source, error = %e, name = "request", "Failed to process payload");
                    }
                }
                Some(event) = this.event_stream.next() => {
                    let source = event.source;
                    if let Err(e) = process_community_event(this, event).await {
                        tracing::error!(%community_id, sender = ?source, error = %e, name = "ev", "Failed to process payload");
                    }
                }
                Some(message) = this.messaging_stream.next() => {
                    let source = message.source;
                    if let Err(e) = this.process_msg_event(message).await {
                        tracing::error!(%community_id, sender = ?source, error = %e, name = "msg", "Failed to process payload");
                    }
                },
                Some(message) = this.join_stream.next() => {
                    let source = message.source;
                    if let Err(e) = this.process_join_event(message).await {
                        tracing::error!(%community_id, sender = ?source, error = %e, name = "join", "Failed to process payload");
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
            }
        }
    }
}

impl CommunityTask {
    async fn load_from_mailbox(&mut self) -> Result<(), Error> {
        // let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config().clone()
        // else {
        //     return Ok(());
        // };
        //
        // let ipfs = self.ipfs.clone();
        // let message_command = self.message_command.clone();
        // let addresses = addresses.clone();
        //
        // let community_id = self.community_id;
        //
        // for channel in self.document.channels.values_mut() {
        //     let channel_id = channel.id;
        //
        //     let mut mailbox = BTreeMap::new();
        //     let mut providers = vec![];
        //     for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
        //         let (tx, rx) = futures::channel::oneshot::channel();
        //         let _ = message_command
        //             .clone()
        //             .send(MessageCommand::FetchMailbox {
        //                 peer_id,
        //                 conversation_id: channel_id,
        //                 response: tx,
        //             })
        //             .await;
        //
        //         match rx.timeout(SHUTTLE_TIMEOUT).await {
        //             Ok(Ok(Ok(list))) => {
        //                 providers.push(peer_id);
        //                 mailbox.extend(list);
        //                 break;
        //             }
        //             Ok(Ok(Err(e))) => {
        //                 tracing::error!(
        //                     "unable to get mailbox to community channel {channel_id} from {peer_id}: {e}"
        //                 );
        //                 break;
        //             }
        //             Ok(Err(_)) => {
        //                 tracing::error!("Channel been unexpectedly closed for {peer_id}");
        //                 continue;
        //             }
        //             Err(_) => {
        //                 tracing::error!("Request timed out for {peer_id}");
        //                 continue;
        //             }
        //         }
        //     }
        //
        //     let community_mailbox = mailbox
        //         .into_iter()
        //         .filter_map(|(id, cid)| {
        //             let id = Uuid::from_str(&id).ok()?;
        //             Some((id, cid))
        //         })
        //         .collect::<BTreeMap<Uuid, Cid>>();
        //
        //     let mut messages =
        //         FuturesUnordered::from_iter(community_mailbox.into_iter().map(|(id, cid)| {
        //             let ipfs = ipfs.clone();
        //             async move {
        //                 ipfs.fetch(&cid).recursive().await?;
        //                 Ok((id, cid))
        //             }
        //             .boxed()
        //         }))
        //         .filter_map(|res: Result<_, anyhow::Error>| async move { res.ok() })
        //         .filter_map(|(_, cid)| {
        //             let ipfs = ipfs.clone();
        //             let providers = providers.clone();
        //             let addresses = addresses.clone();
        //             let message_command = message_command.clone();
        //             async move {
        //                 let message_document = ipfs
        //                     .get_dag(cid)
        //                     .providers(&providers)
        //                     .deserialized::<MessageDocument>()
        //                     .await
        //                     .ok()?;
        //
        //                 if !message_document.verify() {
        //                     return None;
        //                 }
        //
        //                 for peer_id in addresses.into_iter().filter_map(|addr| addr.peer_id()) {
        //                     let _ = message_command
        //                         .clone()
        //                         .send(MessageCommand::MessageDelivered {
        //                             peer_id,
        //                             conversation_id: channel_id,
        //                             message_id: message_document.id,
        //                         })
        //                         .await;
        //                 }
        //                 Some(message_document)
        //             }
        //         })
        //         .collect::<Vec<_>>()
        //         .await;
        //
        //     messages.sort_by(|a, b| b.cmp(a));
        //
        //     for message in messages {
        //         if !message.verify() {
        //             continue;
        //         }
        //         let message_id = message.id;
        //         match channel
        //             .contains(&self.ipfs, message_id)
        //             .await
        //             .unwrap_or_default()
        //         {
        //             true => {
        //                 let current_message =
        //                     channel.get_message_document(&self.ipfs, message_id).await?;
        //
        //                 channel
        //                     .update_message_document(&self.ipfs, &message)
        //                     .await?;
        //
        //                 let is_edited = matches!((message.modified, current_message.modified), (Some(modified), Some(current_modified)) if modified > current_modified )
        //                     | matches!(
        //                         (message.modified, current_message.modified),
        //                         (Some(_), None)
        //                     );
        //
        //                 match is_edited {
        //                     true => {
        //                         let _ = self.event_broadcast.send(
        //                             MessageEventKind::CommunityMessageEdited {
        //                                 community_id,
        //                                 channel_id,
        //                                 message_id,
        //                             },
        //                         );
        //                     }
        //                     false => {
        //                         //TODO: Emit event showing message was updated in some way
        //                     }
        //                 }
        //             }
        //             false => {
        //                 channel
        //                     .insert_message_document(&self.ipfs, &message)
        //                     .await?;
        //
        //                 let _ =
        //                     self.event_broadcast
        //                         .send(MessageEventKind::CommunityMessageReceived {
        //                             community_id,
        //                             channel_id,
        //                             message_id,
        //                         });
        //             }
        //         }
        //     }
        // }
        //
        // self.set_document().await?;

        Ok(())
    }

    async fn process_command(&mut self, command: CommunityTaskCommand) {
        match command {
            CommunityTaskCommand::LeaveCommunity { response } => {
                let result = self.leave_community().await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityIcon { response } => {
                let result = self.get_community_image(ConversationImageType::Icon).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityBanner { response } => {
                let result = self
                    .get_community_image(ConversationImageType::Banner)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EditCommunityIcon { response, location } => {
                let result = self
                    .edit_community_image(location, ConversationImageType::Icon)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EditCommunityBanner { response, location } => {
                let result = self
                    .edit_community_image(location, ConversationImageType::Banner)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::CreateCommunityInvite {
                response,
                target_user,
                expiry,
            } => {
                let result = self.create_community_invite(target_user, expiry).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::DeleteCommunityInvite {
                response,
                invite_id,
            } => {
                let result = self.delete_community_invite(invite_id).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityInvite {
                response,
                invite_id,
            } => {
                let result = self.get_community_invite(invite_id).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EditCommunityInvite {
                response,
                invite_id,
                invite,
            } => {
                let result = self.edit_community_invite(invite_id, invite).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::CreateCommunityRole { response, name } => {
                let result = self.create_community_role(name).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::DeleteCommunityRole { response, role_id } => {
                let result = self.delete_community_role(role_id).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityRole { response, role_id } => {
                let result = self.get_community_role(role_id).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EditCommunityRoleName {
                response,
                role_id,
                new_name,
            } => {
                let result = self.edit_community_role_name(role_id, new_name).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GrantCommunityRole {
                response,
                role_id,
                user,
            } => {
                let result = self.grant_community_role(role_id, user).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::RevokeCommunityRole {
                response,
                role_id,
                user,
            } => {
                let result = self.revoke_community_role(role_id, user).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::CreateCommunityChannel {
                response,
                channel_name,
                channel_type,
            } => {
                let result = self
                    .create_community_channel(channel_name, channel_type)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::DeleteCommunityChannel {
                response,
                channel_id,
            } => {
                let result = self.delete_community_channel(channel_id).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityChannel {
                response,
                channel_id,
            } => {
                let result = self.get_community_channel(channel_id).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EditCommunityName { response, name } => {
                let result = self.edit_community_name(name).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EditCommunityDescription {
                description,
                response,
            } => {
                let result = self.edit_community_description(description).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GrantCommunityPermission {
                response,
                permission,
                role_id,
            } => {
                let result = self.grant_community_permission(permission, role_id).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::RevokeCommunityPermission {
                response,
                permission,
                role_id,
            } => {
                let result = self.revoke_community_permission(permission, role_id).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GrantCommunityPermissionForAll {
                response,
                permission,
            } => {
                let result = self.grant_community_permission_for_all(permission).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::RevokeCommunityPermissionForAll {
                response,
                permission,
            } => {
                let result = self.revoke_community_permission_for_all(permission).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::HasCommunityPermission {
                response,
                permission,
                member,
            } => {
                let result = self.has_community_permission(permission, member).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::RemoveCommunityMember { response, member } => {
                let result = self.remove_community_member(member).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EditCommunityChannelName {
                response,
                channel_id,
                name,
            } => {
                let result = self.edit_community_channel_name(channel_id, name).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EditCommunityChannelDescription {
                response,
                channel_id,
                description,
            } => {
                let result = self
                    .edit_community_channel_description(channel_id, description)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GrantCommunityChannelPermission {
                response,
                channel_id,
                permission,
                role_id,
            } => {
                let result = self
                    .grant_community_channel_permission(channel_id, permission, role_id)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::RevokeCommunityChannelPermission {
                response,
                channel_id,
                permission,
                role_id,
            } => {
                let result = self
                    .revoke_community_channel_permission(channel_id, permission, role_id)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GrantCommunityChannelPermissionForAll {
                response,
                channel_id,
                permission,
            } => {
                let result = self
                    .grant_community_channel_permission_for_all(channel_id, permission)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::RevokeCommunityChannelPermissionForAll {
                response,
                channel_id,
                permission,
            } => {
                let result = self
                    .revoke_community_channel_permission_for_all(channel_id, permission)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::HasCommunityChannelPermission {
                response,
                channel_id,
                permission,
                member,
            } => {
                let result = self
                    .has_community_channel_permission(channel_id, permission, member)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityChannelMessage {
                channel_id,
                message_id,
                response,
            } => {
                let result = self
                    .get_community_channel_message(channel_id, message_id)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityChannelMessages {
                channel_id,
                options,
                response,
            } => {
                let result = self
                    .get_community_channel_messages(channel_id, options)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityChannelMessageCount {
                channel_id,
                response,
            } => {
                let result = self.get_community_channel_message_count(channel_id).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityChannelMessageReference {
                channel_id,
                message_id,
                response,
            } => {
                let result = self
                    .get_community_channel_message_reference(channel_id, message_id)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::GetCommunityChannelMessageReferences {
                channel_id,
                options,
                response,
            } => {
                let result = self
                    .get_community_channel_message_references(channel_id, options)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::CommunityChannelMessageStatus {
                channel_id,
                message_id,
                response,
            } => {
                let result = self
                    .community_channel_message_status(channel_id, message_id)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::SendCommunityChannelMessage {
                channel_id,
                message,
                response,
            } => {
                let result = self
                    .send_community_channel_message(channel_id, message)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EditCommunityChannelMessage {
                channel_id,
                message_id,
                message,
                response,
            } => {
                let result = self
                    .edit_community_channel_message(channel_id, message_id, message)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::ReplyToCommunityChannelMessage {
                channel_id,
                message_id,
                message,
                response,
            } => {
                let result = self
                    .reply_to_community_channel_message(channel_id, message_id, message)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::DeleteCommunityChannelMessage {
                channel_id,
                message_id,
                response,
            } => {
                let result = self
                    .delete_community_channel_message(channel_id, message_id)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::PinCommunityChannelMessage {
                channel_id,
                message_id,
                state,
                response,
            } => {
                let result = self
                    .pin_community_channel_message(channel_id, message_id, state)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::ReactToCommunityChannelMessage {
                channel_id,
                message_id,
                state,
                emoji,
                response,
            } => {
                let result = self
                    .react_to_community_channel_message(channel_id, message_id, state, emoji)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::SendCommunityChannelMesssageEvent {
                channel_id,
                event,
                response,
            } => {
                let result = self
                    .send_community_channel_messsage_event(channel_id, event)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::CancelCommunityChannelMesssageEvent {
                channel_id,
                event,
                response,
            } => {
                let result = self
                    .cancel_community_channel_messsage_event(channel_id, event)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::AttachToCommunityChannelMessage {
                channel_id,
                message_id,
                locations,
                message,
                response,
            } => {
                let result = self
                    .attach_to_community_channel_message(channel_id, message_id, locations, message)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::DownloadFromCommunityChannelMessage {
                channel_id,
                message_id,
                file,
                path,
                response,
            } => {
                let result = self
                    .download_from_community_channel_message(channel_id, message_id, file, path)
                    .await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::DownloadStreamFromCommunityChannelMessage {
                channel_id,
                message_id,
                file,
                response,
            } => {
                let result = self
                    .download_stream_from_community_channel_message(channel_id, message_id, file)
                    .await;
                let _ = response.send(result);
            }

            CommunityTaskCommand::SendJoinedCommunityEvent { response } => {
                let event = CommunityMessagingEvents::JoinedCommunity {
                    community_id: self.community_id,
                    user: self.identity.did_key(),
                };
                let result = self.publish(None, event, true).await;
                let _ = response.send(result);
            }
            CommunityTaskCommand::EventHandler { response } => {
                let sender = self.event_broadcast.clone();
                let _ = response.send(sender);
            }
            CommunityTaskCommand::Delete { response } => {
                let result = self.delete().await;
                let _ = response.send(result);
            }
        }
    }
}

impl CommunityTask {
    pub async fn delete(&mut self) -> Result<(), Error> {
        // TODO: Maybe announce to network of the local node removal here

        for channel in self.document.channels.values_mut() {
            channel.messages.take();
        }

        self.document.deleted = true;
        self.set_document().await?;
        if let Ok(mut ks_map) = self.root.get_keystore_map().await {
            if ks_map.remove(&self.community_id.to_string()).is_some() {
                if let Err(e) = self.root.set_keystore_map(ks_map).await {
                    tracing::warn!(community_id = %self.community_id, error = %e, "failed to remove keystore");
                }
            }
        }
        self.terminate.cancel();
        Ok(())
    }
    pub async fn set_keystore(&mut self, keystore: Option<&Keystore>) -> Result<(), Error> {
        let mut map = self.root.get_keystore_map().await?;

        let id = self.community_id.to_string();

        let keystore = keystore.unwrap_or(&self.keystore);

        let cid = self.ipfs.put_dag(keystore).await?;

        map.insert(id, cid);

        self.root.set_keystore_map(map).await
    }

    pub async fn set_document(&mut self) -> Result<(), Error> {
        let keypair = self.root.keypair();
        let did = keypair.to_did()?;
        if self.document.owner.eq(&did) {
            self.document.sign(keypair)?;
        }

        self.document.verify()?;

        self.root.set_community_document(&self.document).await?;
        self.identity.export_root_document().await?;
        Ok(())
    }

    pub async fn replace_document(&mut self, mut document: CommunityDocument) -> Result<(), Error> {
        let keypair = self.root.keypair();
        let did = keypair.to_did()?;
        if self.document.owner.eq(&did) {
            document.sign(keypair)?;
        }

        document.verify()?;

        self.root.set_community_document(&document).await?;
        self.identity.export_root_document().await?;
        self.document = document;
        Ok(())
    }

    async fn send_single_community_event(
        &mut self,
        did_key: &DID,
        event: ConversationEvents,
    ) -> Result<(), Error> {
        let keypair = self.root.keypair();

        let payload = PayloadBuilder::new(keypair, event)
            .add_recipient(did_key)?
            .from_ipfs(&self.ipfs)
            .await?;

        let bytes = payload.to_bytes()?;

        let peer_id = did_key.to_peer_id()?;
        let peers = self.ipfs.pubsub_peers(Some(did_key.messaging())).await?;

        let mut time = true;
        let timer = Instant::now();
        if !peers.contains(&peer_id)
            || (peers.contains(&peer_id)
                && self
                    .ipfs
                    .pubsub_publish(did_key.messaging(), bytes.clone())
                    .await
                    .is_err())
        {
            tracing::warn!(id=%&self.community_id, "Unable to publish to topic. Queuing event");
            self.queue_event(
                did_key.clone(),
                QueueItem::direct(None, peer_id, did_key.messaging(), bytes.clone()),
            )
            .await;
            time = false;
        }
        if time {
            let end = timer.elapsed();
            tracing::info!(id=%self.community_id, "Event sent to {did_key}");
            tracing::trace!(id=%self.community_id, "Took {}ms to send event", end.as_millis());
        }

        Ok(())
    }

    async fn process_msg_event(&mut self, msg: Message) -> Result<(), Error> {
        let data = PayloadMessage::<CommunityMessagingEvents>::from_bytes(&msg.data)?;
        let sender = data.sender().to_did()?;

        let keypair = self.root.keypair();

        let id = self.community_id;

        let event = match self.keystore.get_latest(keypair, &sender) {
            Ok(key) => data.message_from_key(&key)?,
            Err(Error::PublicKeyDoesntExist) => {
                // match data.message(keypair) {
                //     Ok(message) => {
                //         message
                //     }
                //     _ => {
                // If we are not able to get the latest key from the store, this is because we are still awaiting on the response from the key exchange
                // So what we should so instead is set aside the payload until we receive the key exchange then attempt to process it again

                // Note: We can set aside the data without the payload being owned directly due to the data already been verified
                //       so we can own the data directly without worrying about the lifetime
                //       however, we may want to eventually validate the data to ensure it havent been tampered in some way
                //       while waiting for the response.
                let bytes = data.to_bytes()?;
                self.pending_key_exchange
                    .entry(sender)
                    .or_default()
                    .push((bytes, false));

                // Maybe send a request? Although we could, we should check to determine if one was previously sent or queued first,
                // but for now we can leave this commented until the queue is removed and refactored.
                // _ = self.request_key(id, &data.sender()).await;

                // Note: We will mark this as `Ok` since this is pending request to be resolved
                return Ok(());
                //     }
                // }
            }
            Err(e) => {
                tracing::warn!(id = %id, sender = %data.sender(), error = %e, "Failed to obtain key");
                return Err(e);
            }
        };

        message_event(self, &sender, event).await?;

        Ok(())
    }
    async fn process_join_event(&mut self, msg: Message) -> Result<(), Error> {
        let data = PayloadMessage::<CommunityJoinEvents>::from_bytes(&msg.data)?;
        let community_id = self.community_id;
        let sender = data.sender().to_did()?;

        match data.message(None)? {
            CommunityJoinEvents::Join => {
                let now = Utc::now();

                if !self.document.invites.iter().any(|(_, invite)| {
                    invite.expiry.is_none_or(|expiry| expiry > now)
                        && invite
                            .target_user
                            .as_ref()
                            .is_none_or(|target| &sender == target)
                }) {
                    self.send_single_community_event(
                        &sender,
                        ConversationEvents::JoinCommunity {
                            community_id,
                            community_document: None,
                        },
                    )
                    .await?;
                    return Ok(());
                }

                self.document.members.insert(sender.clone());

                self.document.invites.retain(|_, invite| {
                    !invite
                        .target_user
                        .as_ref()
                        .is_some_and(|target| &sender == target)
                });

                self.set_document().await?;

                self.send_single_community_event(
                    &sender,
                    ConversationEvents::JoinCommunity {
                        community_id: self.community_id,
                        community_document: Some(self.document.clone()),
                    },
                )
                .await?;

                if !self.discovery.contains(&sender).await {
                    let _ = self.discovery.insert(&sender).await;
                }
                if let Err(_e) = self.request_key(&sender).await {}
            }
            CommunityJoinEvents::DeleteInvite { invite_id } => {
                let invite_id = invite_id.to_string();
                let invite = self
                    .document
                    .invites
                    .get(&invite_id)
                    .ok_or(Error::CommunityInviteDoesntExist)?
                    .clone();

                if !invite
                    .target_user
                    .clone()
                    .is_some_and(|target| target == sender)
                {
                    return Err(Error::InvalidCommunityInvite);
                }

                self.document.invites.swap_remove(&invite_id);
                self.set_document().await?;

                self.send_single_community_event(
                    &sender,
                    ConversationEvents::DeleteCommunityInvite {
                        community_id: self.community_id,
                        invite,
                    },
                )
                .await?;
            }
        }
        Ok(())
    }

    fn community_key(&self, member: Option<&DID>) -> Result<Vec<u8>, Error> {
        let keypair = self.root.keypair();
        let own_did = self.identity.did_key();

        let recipient = member.unwrap_or(&own_did);

        self.keystore.get_latest(keypair, recipient)
    }

    async fn request_key(&mut self, did: &DID) -> Result<(), Error> {
        let request = ConversationRequestResponse::Request {
            conversation_id: self.community_id,
            kind: ConversationRequestKind::Key,
        };

        let community = &self.document;

        if !community.participants().contains(did) {
            //TODO: user is not a recipient of the conversation
            return Err(Error::PublicKeyInvalid);
        }

        let keypair = self.root.keypair();

        let payload = PayloadBuilder::new(keypair, request)
            .add_recipient(did)?
            .from_ipfs(&self.ipfs)
            .await?;

        let bytes = payload.to_bytes()?;

        let topic = community.exchange_topic(did);

        let peers = self.ipfs.pubsub_peers(Some(topic.clone())).await?;
        let peer_id = did.to_peer_id()?;
        if !peers.contains(&peer_id)
            || (peers.contains(&peer_id)
                && self
                    .ipfs
                    .pubsub_publish(topic.clone(), bytes.clone())
                    .await
                    .is_err())
        {
            tracing::warn!(id = %self.community_id, "Unable to publish to topic");
            self.queue_event(
                did.clone(),
                QueueItem::direct(None, peer_id, topic.clone(), bytes),
            )
            .await;
        }

        // TODO: Store request locally and hold any messages and events until key is received from peer

        Ok(())
    }

    pub async fn send_message_event(&self, event: CommunityMessagingEvents) -> Result<(), Error> {
        let key = self.community_key(None)?;

        let recipients = self.document.participants();

        let payload = PayloadBuilder::new(self.root.keypair(), event)
            .add_recipients(recipients)?
            .set_key(key)
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
                tracing::error!(id=%self.community_id, "Unable to send event: {e}");
            }
        }
        Ok(())
    }

    pub async fn leave_community(&mut self) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        self.document.members.swap_remove(own_did);
        self.document.roles.iter_mut().for_each(|(_, r)| {
            r.members.swap_remove(own_did);
        });
        self.set_document().await?;

        let _ = self.event_broadcast.send(MessageEventKind::LeftCommunity {
            community_id: self.community_id,
        });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::LeaveCommunity,
            },
            true,
        )
        .await
    }
    async fn get_community_image(
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
    async fn edit_community_image(
        &mut self,
        location: Location,
        image_type: ConversationImageType,
    ) -> Result<(), Error> {
        let max_size = match image_type {
            ConversationImageType::Banner => MAX_CONVERSATION_BANNER_SIZE,
            ConversationImageType::Icon => MAX_CONVERSATION_ICON_SIZE,
        };
        let own_did = &self.identity.did_key();
        match image_type {
            ConversationImageType::Icon => {
                if !self
                    .document
                    .has_permission(own_did, &CommunityPermission::EditIcon)
                {
                    return Err(Error::Unauthorized);
                }
            }
            ConversationImageType::Banner => {
                if !self
                    .document
                    .has_permission(own_did, &CommunityPermission::EditBanner)
                {
                    return Err(Error::Unauthorized);
                }
            }
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

                let document = FileDocument::new(&file);
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
                        .and_then(std::ffi::OsStr::to_str)
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
                CommunityUpdateKind::EditIcon
            }
            ConversationImageType::Banner => {
                self.document.banner.replace(cid);
                CommunityUpdateKind::EditBanner
            }
        };

        self.set_document().await?;

        let event = CommunityMessagingEvents::UpdateCommunity {
            community: self.document.clone(),
            kind,
        };

        let message_event = match image_type {
            ConversationImageType::Icon => MessageEventKind::EditedCommunityIcon {
                community_id: self.community_id,
            },
            ConversationImageType::Banner => MessageEventKind::EditedCommunityBanner {
                community_id: self.community_id,
            },
        };

        let _ = self.event_broadcast.send(message_event);

        self.publish(None, event, true).await
    }

    pub async fn create_community_invite(
        &mut self,
        target_user: Option<DID>,
        expiry: Option<DateTime<Utc>>,
    ) -> Result<CommunityInvite, Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::CreateInvites)
        {
            return Err(Error::Unauthorized);
        }

        if let Some(target) = &target_user {
            if self.document.members.contains(target) {
                return Err(Error::AlreadyCommunityMember);
            }
        }

        let invite_doc = CommunityInviteDocument::new(target_user.clone(), expiry);
        self.document
            .invites
            .insert(invite_doc.id.to_string(), invite_doc.clone());

        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::CreatedCommunityInvite {
                community_id: self.community_id,
                invite: CommunityInvite::from(invite_doc.clone()),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::CreateCommunityInvite {
                    invite: invite_doc.clone(),
                },
            },
            true,
        )
        .await?;

        if let Some(did_key) = target_user {
            self.send_single_community_event(
                &did_key.clone(),
                ConversationEvents::NewCommunityInvite {
                    community_id: self.community_id,
                    invite: invite_doc.clone(),
                },
            )
            .await?;
        }

        Ok(CommunityInvite::from(invite_doc))
    }
    pub async fn delete_community_invite(&mut self, invite_id: Uuid) -> Result<(), Error> {
        let own_did = &self.identity.did_key();

        let mut is_targeting_self = false;
        if let Some(invite) = self.document.invites.get(&invite_id.to_string()) {
            if let Some(target) = &invite.target_user {
                if target == own_did {
                    is_targeting_self = true;
                }
            }
        }
        if !is_targeting_self
            && !self
                .document
                .has_permission(own_did, &CommunityPermission::DeleteInvites)
        {
            return Err(Error::Unauthorized);
        }

        let invite = self
            .document
            .invites
            .get(&invite_id.to_string())
            .ok_or(Error::CommunityInviteDoesntExist)?
            .clone();
        self.document.invites.swap_remove(&invite_id.to_string());
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::DeletedCommunityInvite {
                community_id: self.community_id,
                invite_id,
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::DeleteCommunityInvite { invite_id },
            },
            true,
        )
        .await?;

        if let Some(did_key) = &invite.target_user {
            self.send_single_community_event(
                &did_key.clone(),
                ConversationEvents::DeleteCommunityInvite {
                    community_id: self.community_id,
                    invite,
                },
            )
            .await?;
        }

        Ok(())
    }
    pub async fn get_community_invite(
        &mut self,
        invite_id: Uuid,
    ) -> Result<CommunityInvite, Error> {
        match self.document.invites.get(&invite_id.to_string()) {
            Some(invite_doc) => Ok(CommunityInvite::from(invite_doc.clone())),
            None => Err(Error::CommunityInviteDoesntExist),
        }
    }
    pub async fn edit_community_invite(
        &mut self,
        invite_id: Uuid,
        invite: CommunityInvite,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::EditInvites)
        {
            return Err(Error::Unauthorized);
        }

        let invite_doc = self
            .document
            .invites
            .get_mut(&invite_id.to_string())
            .ok_or(Error::CommunityInviteDoesntExist)?;
        invite_doc.target_user = invite.target_user().cloned();
        invite_doc.expiry = invite.expiry();
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::EditedCommunityInvite {
                community_id: self.community_id,
                invite_id,
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::EditCommunityInvite { invite_id },
            },
            true,
        )
        .await?;

        let invite = self
            .document
            .invites
            .get(&invite_id.to_string())
            .ok_or(Error::CommunityInviteDoesntExist)?;
        if let Some(did_key) = &invite.target_user {
            self.send_single_community_event(
                &did_key.clone(),
                ConversationEvents::NewCommunityInvite {
                    community_id: self.community_id,
                    invite: invite.clone(),
                },
            )
            .await?;
        }

        Ok(())
    }

    pub async fn create_community_role(&mut self, name: String) -> Result<CommunityRole, Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::CreateRoles)
        {
            return Err(Error::Unauthorized);
        }

        let role = CommunityRoleDocument::new(name.to_owned());
        self.document
            .roles
            .insert(role.id.to_string(), role.clone());
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::CreatedCommunityRole {
                community_id: self.community_id,
                role: CommunityRole::from(role.clone()),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::CreateCommunityRole { role: role.clone() },
            },
            true,
        )
        .await?;

        Ok(CommunityRole::from(role))
    }
    pub async fn delete_community_role(&mut self, role_id: RoleId) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::DeleteRoles)
        {
            return Err(Error::Unauthorized);
        }

        self.document.roles.swap_remove(&role_id.to_string());
        let _ = self
            .document
            .permissions
            .iter_mut()
            .map(|(_, roles)| roles.swap_remove(&role_id));
        let _ = self.document.channels.iter_mut().map(|(_, channel_doc)| {
            channel_doc
                .permissions
                .iter_mut()
                .map(|(_, roles)| roles.swap_remove(&role_id))
        });
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::DeletedCommunityRole {
                community_id: self.community_id,
                role_id,
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::DeleteCommunityRole { role_id },
            },
            true,
        )
        .await
    }
    pub async fn get_community_role(&mut self, role_id: RoleId) -> Result<CommunityRole, Error> {
        let role = self
            .document
            .roles
            .get(&role_id.to_string())
            .ok_or(Error::CommunityRoleDoesntExist)?;
        Ok(CommunityRole::from(role.clone()))
    }
    pub async fn edit_community_role_name(
        &mut self,
        role_id: RoleId,
        new_name: String,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::EditRoles)
        {
            return Err(Error::Unauthorized);
        }

        self.document
            .roles
            .get_mut(&role_id.to_string())
            .ok_or(Error::CommunityRoleDoesntExist)?
            .name = new_name;
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::EditedCommunityRole {
                community_id: self.community_id,
                role_id,
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::EditCommunityRole { role_id },
            },
            true,
        )
        .await
    }
    pub async fn grant_community_role(&mut self, role_id: RoleId, user: DID) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::GrantRoles)
        {
            return Err(Error::Unauthorized);
        }
        if !self.document.members.contains(&user) {
            return Err(Error::InvalidCommunityMember);
        }

        self.document
            .roles
            .get_mut(&role_id.to_string())
            .ok_or(Error::CommunityRoleDoesntExist)?
            .members
            .insert(user.clone());
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::GrantedCommunityRole {
                community_id: self.community_id,
                role_id,
                user: user.clone(),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::GrantCommunityRole { role_id, user },
            },
            true,
        )
        .await
    }
    pub async fn revoke_community_role(&mut self, role_id: RoleId, user: DID) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::RevokeRoles)
        {
            return Err(Error::Unauthorized);
        }

        self.document
            .roles
            .get_mut(&role_id.to_string())
            .ok_or(Error::CommunityRoleDoesntExist)?
            .members
            .swap_remove(&user);
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::RevokedCommunityRole {
                community_id: self.community_id,
                role_id,
                user: user.clone(),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::RevokeCommunityRole { role_id, user },
            },
            true,
        )
        .await
    }

    pub async fn create_community_channel(
        &mut self,
        channel_name: String,
        channel_type: CommunityChannelType,
    ) -> Result<CommunityChannel, Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::CreateChannels)
        {
            return Err(Error::Unauthorized);
        }

        if self.document.channels.len() >= MAX_COMMUNITY_CHANNELS {
            return Err(Error::CommunityChannelLimitReached);
        }
        let channel_doc =
            CommunityChannelDocument::new(channel_name.to_owned(), None, channel_type);
        self.document
            .channels
            .insert(channel_doc.id.to_string(), channel_doc.clone());
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::CreatedCommunityChannel {
                community_id: self.community_id,
                channel: CommunityChannel::from(channel_doc.clone()),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::CreateCommunityChannel {
                    channel: channel_doc.clone(),
                },
            },
            true,
        )
        .await?;

        Ok(CommunityChannel::from(channel_doc))
    }
    pub async fn delete_community_channel(&mut self, channel_id: Uuid) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::DeleteChannels)
        {
            return Err(Error::Unauthorized);
        }

        self.document.channels.swap_remove(&channel_id.to_string());
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::DeletedCommunityChannel {
                community_id: self.community_id,
                channel_id,
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::DeleteCommunityChannel { channel_id },
            },
            true,
        )
        .await
    }
    pub async fn get_community_channel(
        &mut self,
        channel_id: Uuid,
    ) -> Result<CommunityChannel, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        let channel_doc = self
            .document
            .channels
            .get(&channel_id.to_string())
            .ok_or(Error::CommunityChannelDoesntExist)?;
        Ok(CommunityChannel::from(channel_doc.clone()))
    }

    pub async fn edit_community_name(&mut self, name: String) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::EditName)
        {
            return Err(Error::Unauthorized);
        }

        self.document.name = name.to_owned();
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::EditedCommunityName {
                community_id: self.community_id,
                name: name.to_string(),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::EditCommunityName {
                    name: name.to_string(),
                },
            },
            true,
        )
        .await
    }
    pub async fn edit_community_description(
        &mut self,
        description: Option<String>,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::EditDescription)
        {
            return Err(Error::Unauthorized);
        }

        if let Some(desc) = &description {
            if desc.is_empty() || desc.len() > MAX_COMMUNITY_DESCRIPTION {
                return Err(Error::InvalidLength {
                    context: "description".into(),
                    minimum: Some(1),
                    maximum: Some(MAX_COMMUNITY_DESCRIPTION),
                    current: desc.len(),
                });
            }
        }

        self.document.description = description.clone();
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::EditedCommunityDescription {
                community_id: self.community_id,
                description: self.document.description.clone(),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::EditCommunityDescription {
                    description: self.document.description.clone(),
                },
            },
            true,
        )
        .await
    }

    pub async fn grant_community_permission(
        &mut self,
        permission: String,
        role_id: RoleId,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::GrantPermissions)
        {
            return Err(Error::Unauthorized);
        }

        let permissions: Vec<String> = CommunityPermission::sub_permissions(&permission)
            .iter()
            .map(|p| p.to_string())
            .collect();
        if permissions.is_empty() {
            return Err(Error::InvalidPermission);
        }
        for permission in &permissions {
            match self.document.permissions.get_mut(permission) {
                Some(authorized_roles) => {
                    authorized_roles.insert(role_id);
                }
                None => {
                    let mut roles = IndexSet::new();
                    roles.insert(role_id);
                    self.document.permissions.insert(permission.clone(), roles);
                }
            }
        }
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::GrantedCommunityPermission {
                community_id: self.community_id,
                permissions: permissions.clone(),
                role_id,
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::GrantCommunityPermission {
                    permissions,
                    role_id,
                },
            },
            true,
        )
        .await
    }
    pub async fn revoke_community_permission(
        &mut self,
        permission: String,
        role_id: RoleId,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::RevokePermissions)
        {
            return Err(Error::Unauthorized);
        }

        let permissions: Vec<String> = CommunityPermission::sub_permissions(&permission)
            .iter()
            .map(|p| p.to_string())
            .collect();
        if permissions.is_empty() {
            return Err(Error::InvalidPermission);
        }
        for permission in &permissions {
            if let Some(authorized_roles) = self.document.permissions.get_mut(permission) {
                authorized_roles.swap_remove(&role_id);
            }
        }
        self.set_document().await?;
        let _ = self
            .event_broadcast
            .send(MessageEventKind::RevokedCommunityPermission {
                community_id: self.community_id,
                permissions: permissions.clone(),
                role_id,
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::RevokeCommunityPermission {
                    permissions,
                    role_id,
                },
            },
            true,
        )
        .await
    }
    pub async fn grant_community_permission_for_all(
        &mut self,
        permission: String,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::GrantPermissions)
        {
            return Err(Error::Unauthorized);
        }

        let permissions: Vec<String> = CommunityPermission::sub_permissions(&permission)
            .iter()
            .map(|p| p.to_string())
            .collect();
        if permissions.is_empty() {
            return Err(Error::InvalidPermission);
        }
        for permission in &permissions {
            if self.document.permissions.contains_key(permission) {
                self.document.permissions.swap_remove(permission);
                self.set_document().await?;
            } else if permissions.len() == 1 {
                return Err(Error::PermissionAlreadyGranted);
            }
        }

        let _ = self
            .event_broadcast
            .send(MessageEventKind::GrantedCommunityPermissionForAll {
                community_id: self.community_id,
                permissions: permissions.clone(),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::GrantCommunityPermissionForAll { permissions },
            },
            true,
        )
        .await
    }
    pub async fn revoke_community_permission_for_all(
        &mut self,
        permission: String,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::RevokePermissions)
        {
            return Err(Error::Unauthorized);
        }

        let permissions: Vec<String> = CommunityPermission::sub_permissions(&permission)
            .iter()
            .map(|p| p.to_string())
            .collect();
        if permissions.is_empty() {
            return Err(Error::InvalidPermission);
        }
        for permission in &permissions {
            self.document
                .permissions
                .insert(permission.clone(), IndexSet::new());
        }
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::RevokedCommunityPermissionForAll {
                community_id: self.community_id,
                permissions: permissions.clone(),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::RevokeCommunityPermissionForAll { permissions },
            },
            true,
        )
        .await
    }
    pub async fn has_community_permission(
        &mut self,
        permission: String,
        member: DID,
    ) -> Result<bool, Error> {
        Ok(self.document.has_permission(&member, &permission))
    }
    pub async fn remove_community_member(&mut self, member: DID) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::RemoveMembers)
        {
            return Err(Error::Unauthorized);
        }

        self.document.members.swap_remove(&member);
        self.document.roles.iter_mut().for_each(|(_, r)| {
            r.members.swap_remove(&member);
        });
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::RemovedCommunityMember {
                community_id: self.community_id,
                member: member.clone(),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::RemoveCommunityMember { member },
            },
            true,
        )
        .await
    }

    pub async fn edit_community_channel_name(
        &mut self,
        channel_id: Uuid,
        name: String,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::EditChannels)
        {
            return Err(Error::Unauthorized);
        }

        let channel_doc = self
            .document
            .channels
            .get_mut(&channel_id.to_string())
            .ok_or(Error::CommunityChannelDoesntExist)?;
        channel_doc.name = name.to_owned();
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::EditedCommunityChannelName {
                community_id: self.community_id,
                channel_id,
                name: name.to_string(),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::EditCommunityChannelName {
                    channel_id,
                    name: name.to_string(),
                },
            },
            true,
        )
        .await
    }
    pub async fn edit_community_channel_description(
        &mut self,
        channel_id: Uuid,
        description: Option<String>,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::EditChannels)
        {
            return Err(Error::Unauthorized);
        }

        let channel_doc = self
            .document
            .channels
            .get_mut(&channel_id.to_string())
            .ok_or(Error::CommunityChannelDoesntExist)?;
        channel_doc.description = description.clone();
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::EditedCommunityChannelDescription {
                community_id: self.community_id,
                channel_id,
                description: description.clone(),
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::EditCommunityChannelDescription {
                    channel_id,
                    description,
                },
            },
            true,
        )
        .await
    }
    pub async fn grant_community_channel_permission(
        &mut self,
        channel_id: Uuid,
        permission: String,
        role_id: RoleId,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::GrantPermissions)
        {
            return Err(Error::Unauthorized);
        }

        let channel_doc = self
            .document
            .channels
            .get_mut(&channel_id.to_string())
            .ok_or(Error::CommunityChannelDoesntExist)?;
        let permissions: Vec<String> = CommunityChannelPermission::sub_permissions(&permission)
            .iter()
            .map(|p| p.to_string())
            .collect();
        if permissions.is_empty() {
            return Err(Error::InvalidPermission);
        }
        for permission in &permissions {
            match channel_doc.permissions.get_mut(permission) {
                Some(authorized_roles) => {
                    authorized_roles.insert(role_id);
                }
                None => {
                    let mut roles = IndexSet::new();
                    roles.insert(role_id);
                    channel_doc.permissions.insert(permission.clone(), roles);
                }
            }
        }
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::GrantedCommunityChannelPermission {
                community_id: self.community_id,
                channel_id,
                permissions: permissions.clone(),
                role_id,
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::GrantCommunityChannelPermission {
                    channel_id,
                    permissions,
                    role_id,
                },
            },
            true,
        )
        .await
    }
    pub async fn revoke_community_channel_permission(
        &mut self,
        channel_id: Uuid,
        permission: String,
        role_id: RoleId,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::RevokePermissions)
        {
            return Err(Error::Unauthorized);
        }

        let channel_doc = self
            .document
            .channels
            .get_mut(&channel_id.to_string())
            .ok_or(Error::CommunityChannelDoesntExist)?;
        let permissions: Vec<String> = CommunityChannelPermission::sub_permissions(&permission)
            .iter()
            .map(|p| p.to_string())
            .collect();
        if permissions.is_empty() {
            return Err(Error::InvalidPermission);
        }
        for permission in &permissions {
            if let Some(authorized_roles) = channel_doc.permissions.get_mut(permission) {
                authorized_roles.swap_remove(&role_id);
            }
        }
        self.set_document().await?;

        let _ = self
            .event_broadcast
            .send(MessageEventKind::RevokedCommunityChannelPermission {
                community_id: self.community_id,
                channel_id,
                permissions: permissions.clone(),
                role_id,
            });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::RevokeCommunityChannelPermission {
                    channel_id,
                    permissions,
                    role_id,
                },
            },
            true,
        )
        .await
    }
    pub async fn grant_community_channel_permission_for_all(
        &mut self,
        channel_id: Uuid,
        permission: String,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::GrantPermissions)
        {
            return Err(Error::Unauthorized);
        }

        let channel_doc = self
            .document
            .channels
            .get_mut(&channel_id.to_string())
            .ok_or(Error::CommunityChannelDoesntExist)?;
        let permissions: Vec<String> = CommunityChannelPermission::sub_permissions(&permission)
            .iter()
            .map(|p| p.to_string())
            .collect();
        if permissions.is_empty() {
            return Err(Error::InvalidPermission);
        }
        for permission in &permissions {
            if channel_doc.permissions.contains_key(permission) {
                channel_doc.permissions.swap_remove(permission);
            } else if permissions.len() == 1 {
                return Err(Error::PermissionAlreadyGranted);
            }
        }
        self.set_document().await?;
        let _ =
            self.event_broadcast
                .send(MessageEventKind::GrantedCommunityChannelPermissionForAll {
                    community_id: self.community_id,
                    channel_id,
                    permissions: permissions.clone(),
                });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::GrantCommunityChannelPermissionForAll {
                    channel_id,
                    permissions,
                },
            },
            true,
        )
        .await
    }
    pub async fn revoke_community_channel_permission_for_all(
        &mut self,
        channel_id: Uuid,
        permission: String,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::RevokePermissions)
        {
            return Err(Error::Unauthorized);
        }

        let channel_doc = self
            .document
            .channels
            .get_mut(&channel_id.to_string())
            .ok_or(Error::CommunityChannelDoesntExist)?;
        let permissions: Vec<String> = CommunityChannelPermission::sub_permissions(&permission)
            .iter()
            .map(|p| p.to_string())
            .collect();
        if permissions.is_empty() {
            return Err(Error::InvalidPermission);
        }
        for permission in &permissions {
            channel_doc
                .permissions
                .insert(permission.clone(), IndexSet::new());
        }
        self.set_document().await?;

        let _ =
            self.event_broadcast
                .send(MessageEventKind::RevokedCommunityChannelPermissionForAll {
                    community_id: self.community_id,
                    channel_id,
                    permissions: permissions.clone(),
                });

        self.publish(
            None,
            CommunityMessagingEvents::UpdateCommunity {
                community: self.document.clone(),
                kind: CommunityUpdateKind::RevokeCommunityChannelPermissionForAll {
                    channel_id,
                    permissions,
                },
            },
            true,
        )
        .await
    }
    pub async fn has_community_channel_permission(
        &mut self,
        channel_id: Uuid,
        permission: String,
        member: DID,
    ) -> Result<bool, Error> {
        Ok(self
            .document
            .has_channel_permission(&member, &permission, channel_id))
    }

    pub async fn get_community_channel_message(
        &self,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<warp::raygun::Message, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        let keypair = self.root.keypair();
        let keystore = pubkey_or_keystore(self)?;

        match self.document.channels.get(&channel_id.to_string()) {
            Some(channel) => {
                channel
                    .get_message(&self.ipfs, keypair, message_id, keystore.as_ref())
                    .await
            }
            None => Err(Error::CommunityChannelDoesntExist),
        }
    }
    pub async fn get_community_channel_messages(
        &self,
        channel_id: Uuid,
        options: MessageOptions,
    ) -> Result<Messages, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        let keypair = self.root.keypair();
        let keystore = pubkey_or_keystore(self)?;

        match self.document.channels.get(&channel_id.to_string()) {
            None => Err(Error::CommunityChannelDoesntExist),
            Some(channel) => {
                let m_type = options.messages_type();
                match m_type {
                    MessagesType::Stream => {
                        let stream = channel
                            .get_messages_stream(&self.ipfs, keypair, options, keystore)
                            .await?;
                        Ok(Messages::Stream(stream))
                    }
                    MessagesType::List => {
                        let list = channel
                            .get_messages(&self.ipfs, keypair, options, keystore)
                            .await?;
                        Ok(Messages::List(list))
                    }
                    MessagesType::Pages { .. } => {
                        channel
                            .get_messages_pages(&self.ipfs, keypair, options, keystore.as_ref())
                            .await
                    }
                }
            }
        }
    }
    pub async fn get_community_channel_message_count(
        &self,
        channel_id: Uuid,
    ) -> Result<usize, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        match self.document.channels.get(&channel_id.to_string()) {
            None => Err(Error::CommunityChannelDoesntExist),
            Some(channel) => channel.messages_length(&self.ipfs).await,
        }
    }
    pub async fn get_community_channel_message_reference(
        &self,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageReference, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        match self.document.channels.get(&channel_id.to_string()) {
            None => Err(Error::CommunityChannelDoesntExist),
            Some(channel) => channel
                .get_message_document(&self.ipfs, message_id)
                .await
                .map(|document| document.into()),
        }
    }
    pub async fn get_community_channel_message_references(
        &self,
        channel_id: Uuid,
        options: MessageOptions,
    ) -> Result<BoxStream<'static, MessageReference>, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        match self.document.channels.get(&channel_id.to_string()) {
            None => Err(Error::CommunityChannelDoesntExist),
            Some(channel) => {
                channel
                    .get_messages_reference_stream(&self.ipfs, options)
                    .await
            }
        }
    }
    pub async fn community_channel_message_status(
        &self,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageStatus, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        let channel = match self.document.channels.get(&channel_id.to_string()) {
            None => return Err(Error::CommunityChannelDoesntExist),
            Some(c) => c,
        };

        let messages = channel.get_message_list(&self.ipfs).await?;

        if !messages.iter().any(|document| document.id == message_id) {
            return Err(Error::MessageNotFound);
        }

        let _list = self
            .document
            .participants()
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
    pub async fn send_community_channel_message(
        &mut self,
        channel_id: Uuid,
        messages: Vec<String>,
    ) -> Result<Uuid, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::SendMessages,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        if !self.document.channels.contains_key(&channel_id.to_string()) {
            return Err(Error::CommunityChannelDoesntExist);
        }

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

        let keystore = pubkey_or_keystore(&*self)?;

        let message = MessageDocumentBuilder::new(keypair, keystore.as_ref())
            .set_conversation_id(channel_id)
            .set_sender(own_did.clone())
            .set_message(messages.clone())?
            .build()?;

        let message_id = message.id;

        let channel = match self.document.channels.get_mut(&channel_id.to_string()) {
            Some(c) => c,
            None => return Err(Error::CommunityChannelDoesntExist),
        };

        let _message_cid = channel
            .insert_message_document(&self.ipfs, &message)
            .await?;

        // let recipients = self.document.participants();

        self.set_document().await?;

        let event = MessageEventKind::CommunityMessageSent {
            community_id: self.community_id,
            channel_id,
            message_id,
        };

        if let Err(e) = self.event_broadcast.clone().send(event) {
            tracing::error!(conversation_id=%channel_id, error = %e, "Error broadcasting event");
        }

        let message_id = message.id;

        let event = CommunityMessagingEvents::New {
            community_id: self.community_id,
            channel_id,
            message,
        };

        // if !recipients.is_empty() {
        //     if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
        //         for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
        //             let _ = self
        //                 .message_command
        //                 .clone()
        //                 .send(MessageCommand::InsertMessage {
        //                     peer_id,
        //                     conversation_id: channel_id,
        //                     recipients: recipients.iter().cloned().collect(),
        //                     message_id,
        //                     message_cid,
        //                 })
        //                 .await;
        //         }
        //     }
        // }

        self.publish(Some(message_id), event, true)
            .await
            .map(|_| message_id)
    }
    pub async fn edit_community_channel_message(
        &mut self,
        channel_id: Uuid,
        message_id: Uuid,
        messages: Vec<String>,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::SendMessages,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

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

        let channel = match self.document.channels.get_mut(&channel_id.to_string()) {
            Some(c) => c,
            None => return Err(Error::CommunityChannelDoesntExist),
        };

        let mut message_document = channel.get_message_document(&self.ipfs, message_id).await?;

        if message_document.sender() != self.identity.did_key() {
            return Err(Error::InvalidMessage);
        }
        message_document.set_message(keypair, keystore.as_ref(), &messages)?;

        let nonce = message_document.nonce_from_message()?;
        let signature = message_document.signature.expect("message to be signed");

        let _message_cid = channel
            .update_message_document(&self.ipfs, &message_document)
            .await?;

        // let recipients = self.document.participants();

        self.set_document().await?;

        let _ = tx.send(MessageEventKind::CommunityMessageEdited {
            community_id: self.community_id,
            channel_id,
            message_id,
        });

        let event = CommunityMessagingEvents::Edit {
            community_id: self.community_id,
            channel_id,
            message_id,
            modified: message_document.modified.expect("message to be modified"),
            lines: messages,
            nonce: nonce.to_vec(),
            signature: signature.into(),
        };

        // if !recipients.is_empty() {
        //     if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
        //         for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
        //             let _ = self
        //                 .message_command
        //                 .clone()
        //                 .send(MessageCommand::InsertMessage {
        //                     peer_id,
        //                     conversation_id: channel_id,
        //                     recipients: recipients.iter().cloned().collect(),
        //                     message_id,
        //                     message_cid,
        //                 })
        //                 .await;
        //         }
        //     }
        // }

        self.publish(None, event, true).await
    }
    pub async fn reply_to_community_channel_message(
        &mut self,
        channel_id: Uuid,
        message_id: Uuid,
        messages: Vec<String>,
    ) -> Result<Uuid, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::SendMessages,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

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

        let keystore = pubkey_or_keystore(&*self)?;

        let message = MessageDocumentBuilder::new(keypair, keystore.as_ref())
            .set_conversation_id(channel_id)
            .set_sender(own_did.clone())
            .set_replied(message_id)
            .set_message(messages)?
            .build()?;

        let message_id = message.id;

        let channel = match self.document.channels.get_mut(&channel_id.to_string()) {
            Some(c) => c,
            None => return Err(Error::CommunityChannelDoesntExist),
        };

        let _message_cid = channel
            .insert_message_document(&self.ipfs, &message)
            .await?;

        // let recipients = self.document.participants();

        self.set_document().await?;

        let event = MessageEventKind::CommunityMessageSent {
            community_id: self.community_id,
            channel_id,
            message_id,
        };

        if let Err(e) = tx.send(event) {
            tracing::error!(id=%self.community_id, error = %e, "Error broadcasting event");
        }

        let event = CommunityMessagingEvents::New {
            community_id: self.community_id,
            channel_id,
            message,
        };

        // if !recipients.is_empty() {
        //     if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
        //         for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
        //             let _ = self
        //                 .message_command
        //                 .clone()
        //                 .send(MessageCommand::InsertMessage {
        //                     peer_id,
        //                     conversation_id: channel_id,
        //                     recipients: recipients.iter().cloned().collect(),
        //                     message_id,
        //                     message_cid,
        //                 })
        //                 .await;
        //         }
        //     }
        // }

        self.publish(Some(message_id), event, true)
            .await
            .map(|_| message_id)
    }
    pub async fn delete_community_channel_message(
        &mut self,
        channel_id: Uuid,
        message_id: Uuid,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::DeleteMessages)
        {
            return Err(Error::Unauthorized);
        }

        let tx = self.event_broadcast.clone();

        let event = CommunityMessagingEvents::Delete {
            community_id: self.community_id,
            channel_id,
            message_id,
        };

        let channel = match self.document.channels.get_mut(&channel_id.to_string()) {
            Some(c) => c,
            None => return Err(Error::CommunityChannelDoesntExist),
        };

        channel.delete_message(&self.ipfs, message_id).await?;

        self.set_document().await?;

        // if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
        //     for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
        //         let _ = self
        //             .message_command
        //             .clone()
        //             .send(MessageCommand::RemoveMessage {
        //                 peer_id,
        //                 conversation_id: channel_id,
        //                 message_id,
        //             })
        //             .await;
        //     }
        // }

        let _ = tx.send(MessageEventKind::CommunityMessageDeleted {
            community_id: self.community_id,
            channel_id,
            message_id,
        });
        self.publish(None, event, true).await?;
        Ok(())
    }
    pub async fn pin_community_channel_message(
        &mut self,
        channel_id: Uuid,
        message_id: Uuid,
        state: PinState,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self
            .document
            .has_permission(own_did, &CommunityPermission::PinMessages)
        {
            return Err(Error::Unauthorized);
        }

        let tx = self.event_broadcast.clone();

        let own_did = self.identity.did_key();

        let channel = match self.document.channels.get_mut(&channel_id.to_string()) {
            Some(c) => c,
            None => return Err(Error::CommunityChannelDoesntExist),
        };

        let mut message_document = channel.get_message_document(&self.ipfs, message_id).await?;

        let event = match state {
            PinState::Pin => {
                if message_document.pinned() {
                    return Ok(());
                }
                message_document.set_pin(true);
                MessageEventKind::CommunityMessagePinned {
                    community_id: self.community_id,
                    channel_id,
                    message_id,
                }
            }
            PinState::Unpin => {
                if !message_document.pinned() {
                    return Ok(());
                }
                message_document.set_pin(false);
                MessageEventKind::CommunityMessageUnpinned {
                    community_id: self.community_id,
                    channel_id,
                    message_id,
                }
            }
        };

        let _message_cid = channel
            .update_message_document(&self.ipfs, &message_document)
            .await?;

        // let recipients = self.document.participants();

        self.set_document().await?;

        let _ = tx.send(event);

        // if !recipients.is_empty() {
        //     if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
        //         for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
        //             let _ = self
        //                 .message_command
        //                 .clone()
        //                 .send(MessageCommand::InsertMessage {
        //                     peer_id,
        //                     conversation_id: channel_id,
        //                     recipients: recipients.iter().cloned().collect(),
        //                     message_id,
        //                     message_cid,
        //                 })
        //                 .await;
        //         }
        //     }
        // }

        let event = CommunityMessagingEvents::Pin {
            community_id: self.community_id,
            channel_id,
            member: own_did,
            message_id,
            state,
        };

        self.publish(None, event, true).await
    }
    pub async fn react_to_community_channel_message(
        &mut self,
        channel_id: Uuid,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        let tx = self.event_broadcast.clone();

        let own_did = self.identity.did_key();

        // let recipients = self.document.participants();

        let channel = match self.document.channels.get_mut(&channel_id.to_string()) {
            Some(c) => c,
            None => return Err(Error::CommunityChannelDoesntExist),
        };

        let mut message_document = channel.get_message_document(&self.ipfs, message_id).await?;

        let message_cid;

        match state {
            ReactionState::Add => {
                message_document.add_reaction(&emoji, own_did.clone())?;

                message_cid = channel
                    .update_message_document(&self.ipfs, &message_document)
                    .await?;
                self.set_document().await?;

                _ = tx.send(MessageEventKind::CommunityMessageReactionAdded {
                    community_id: self.community_id,
                    channel_id,
                    message_id,
                    did_key: own_did.clone(),
                    reaction: emoji.clone(),
                });
            }
            ReactionState::Remove => {
                message_document.remove_reaction(&emoji, own_did.clone())?;

                message_cid = channel
                    .update_message_document(&self.ipfs, &message_document)
                    .await?;

                self.set_document().await?;

                let _ = tx.send(MessageEventKind::CommunityMessageReactionRemoved {
                    community_id: self.community_id,
                    channel_id,
                    message_id,
                    did_key: own_did.clone(),
                    reaction: emoji.clone(),
                });
            }
        }

        let event = CommunityMessagingEvents::React {
            community_id: self.community_id,
            channel_id,
            reactor: own_did,
            message_id,
            state,
            emoji,
        };

        _ = message_cid;

        // if !recipients.is_empty() {
        //     if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
        //         for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
        //             let _ = self
        //                 .message_command
        //                 .clone()
        //                 .send(MessageCommand::InsertMessage {
        //                     peer_id,
        //                     conversation_id: channel_id,
        //                     recipients: recipients.iter().cloned().collect(),
        //                     message_id,
        //                     message_cid,
        //                 })
        //                 .await;
        //         }
        //     }
        // }

        self.publish(None, event, true).await
    }
    pub async fn send_community_channel_messsage_event(
        &mut self,
        channel_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::SendMessages,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }
        let event = CommunityMessagingEvents::Event {
            community_id: self.community_id,
            channel_id,
            member: own_did.clone(),
            event,
            cancelled: false,
        };
        self.send_message_event(event).await
    }
    pub async fn cancel_community_channel_messsage_event(
        &mut self,
        channel_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        let member = self.identity.did_key();
        let event = CommunityMessagingEvents::Event {
            community_id: self.community_id,
            channel_id,
            member,
            event,
            cancelled: true,
        };
        self.send_message_event(event).await
    }
    pub async fn attach_to_community_channel_message(
        &mut self,
        channel_id: Uuid,
        message_id: Option<Uuid>,
        locations: Vec<Location>,
        messages: Vec<String>,
    ) -> Result<(Uuid, AttachmentEventStream), Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::SendMessages,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::SendAttachments,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        let keystore = pubkey_or_keystore(&*self)?;

        let stream = AttachmentStream::new(
            self.root.keypair(),
            &self.identity.did_key(),
            &self.file,
            channel_id,
            keystore,
            self.attachment_tx.clone(),
        )
        .set_reply(message_id)
        .set_locations(locations)?
        .set_lines(messages)?;

        let message_id = stream.message_id();

        Ok((message_id, stream.boxed()))
    }
    pub async fn download_from_community_channel_message(
        &self,
        channel_id: Uuid,
        message_id: Uuid,
        file: String,
        path: PathBuf,
    ) -> Result<ConstellationProgressStream, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        let channel = match self.document.channels.get(&channel_id.to_string()) {
            Some(c) => c,
            None => return Err(Error::CommunityChannelDoesntExist),
        };

        let members = self
            .document
            .participants()
            .iter()
            .filter_map(|did| did.to_peer_id().ok())
            .collect::<Vec<_>>();

        let message = channel.get_message_document(&self.ipfs, message_id).await?;

        if message.message_type != MessageType::Attachment {
            return Err(Error::InvalidMessage);
        }

        let attachment = message
            .attachments()
            .find(|attachment| attachment.name == file)
            .ok_or(Error::FileNotFound)?;

        let stream = attachment.download(&self.ipfs, path, &members, None);

        Ok(stream)
    }
    pub async fn download_stream_from_community_channel_message(
        &self,
        channel_id: Uuid,
        message_id: Uuid,
        file: String,
    ) -> Result<BoxStream<'static, Result<Bytes, std::io::Error>>, Error> {
        let own_did = &self.identity.did_key();
        if !self.document.has_channel_permission(
            own_did,
            &CommunityChannelPermission::ViewChannel,
            channel_id,
        ) {
            return Err(Error::Unauthorized);
        }

        let channel = match self.document.channels.get(&channel_id.to_string()) {
            Some(c) => c,
            None => return Err(Error::CommunityChannelDoesntExist),
        };

        let members = self
            .document
            .participants()
            .iter()
            .filter_map(|did| did.to_peer_id().ok())
            .collect::<Vec<_>>();

        let message = channel.get_message_document(&self.ipfs, message_id).await?;

        if message.message_type != MessageType::Attachment {
            return Err(Error::InvalidMessage);
        }

        let attachment = message
            .attachments()
            .find(|attachment| attachment.name == file)
            .ok_or(Error::FileNotFound)?;

        let stream = attachment.download_stream(&self.ipfs, &members, None);

        Ok(stream)
    }

    async fn store_direct_for_attachment(&mut self, message: MessageDocument) -> Result<(), Error> {
        let channel_id = message.conversation_id;
        let message_id = message.id;

        let channel = match self.document.channels.get_mut(&channel_id.to_string()) {
            Some(c) => c,
            None => return Err(Error::CommunityChannelDoesntExist),
        };

        let _message_cid = channel
            .insert_message_document(&self.ipfs, &message)
            .await?;

        // let recipients = self.document.participants().clone();

        self.set_document().await?;

        let event = MessageEventKind::CommunityMessageSent {
            community_id: self.community_id,
            channel_id,
            message_id,
        };

        if let Err(e) = self.event_broadcast.send(event) {
            tracing::error!(%channel_id, error = %e, "Error broadcasting event");
        }

        let event = CommunityMessagingEvents::New {
            community_id: self.community_id,
            channel_id,
            message,
        };

        // if !recipients.is_empty() {
        //     if let config::Discovery::Shuttle { addresses } = self.discovery.discovery_config() {
        //         for peer_id in addresses.iter().filter_map(|addr| addr.peer_id()) {
        //             let _ = self
        //                 .message_command
        //                 .clone()
        //                 .send(MessageCommand::InsertMessage {
        //                     peer_id,
        //                     conversation_id: channel_id,
        //                     recipients: recipients.iter().cloned().collect(),
        //                     message_id,
        //                     message_cid,
        //                 })
        //                 .await;
        //         }
        //     }
        // }

        self.publish(Some(message_id), event, true).await
    }

    pub async fn publish(
        &mut self,
        message_id: Option<Uuid>,
        event: CommunityMessagingEvents,
        queue: bool,
    ) -> Result<(), Error> {
        let keypair = self.root.keypair();
        let own_did = self.identity.did_key();

        let key = self.community_key(None)?;

        let recipients = self.document.participants();

        let payload = PayloadBuilder::new(keypair, event)
            .add_recipients(recipients.iter().filter(|did| own_did.ne(did)))?
            .set_key(key)
            .from_ipfs(&self.ipfs)
            .await?;

        let peers = self.ipfs.pubsub_peers(Some(self.document.topic())).await?;

        let mut can_publish = false;

        let recipients = self.document.participants().clone();

        let bytes = payload.to_bytes()?;

        for recipient in recipients.iter().filter(|did| own_did.ne(did)) {
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
                                bytes.clone(),
                            ),
                        )
                        .await;
                    }
                }
            };
        }

        if can_publish {
            tracing::trace!(id = %self.community_id, "Payload size: {} bytes", bytes.len());
            let timer = Instant::now();
            let mut time = true;
            if let Err(_e) = self.ipfs.pubsub_publish(self.document.topic(), bytes).await {
                tracing::error!(id = %self.community_id, "Error publishing: {_e}");
                time = false;
            }
            if time {
                let end = timer.elapsed();
                tracing::trace!(id = %self.community_id, "Took {}ms to send event", end.as_millis());
            }
        }

        Ok(())
    }

    async fn queue_event(&mut self, did: DID, queue: QueueItem) {
        self.queue.entry(did).or_default().push(queue);
        self.save_queue().await
    }

    async fn save_queue(&self) {
        let key = format!("{}/{}", self.ipfs.messaging_queue(), self.community_id);
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
}

async fn message_event(
    this: &mut CommunityTask,
    sender: &DID,
    events: CommunityMessagingEvents,
) -> Result<(), Error> {
    let community_id = this.community_id;

    let keypair = this.root.keypair();
    let own_did = this.identity.did_key();

    let keystore = pubkey_or_keystore(&*this)?;

    match events {
        CommunityMessagingEvents::New {
            community_id,
            channel_id,
            message,
        } => {
            message.verify()?;

            let message_id = message.id;

            if !this
                .document
                .participants()
                .contains(&message.sender.to_did())
            {
                return Err(Error::IdentityDoesntExist);
            }

            let channel = match this.document.channels.get_mut(&channel_id.to_string()) {
                Some(c) => c,
                None => return Err(Error::CommunityChannelDoesntExist),
            };

            if channel.contains(&this.ipfs, message_id).await? {
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

            channel
                .insert_message_document(&this.ipfs, &message)
                .await?;

            this.set_document().await?;

            if let Err(e) = this
                .event_broadcast
                .send(MessageEventKind::CommunityMessageReceived {
                    community_id,
                    channel_id,
                    message_id,
                })
            {
                tracing::warn!(%channel_id, "Error broadcasting event: {e}");
            }
        }
        CommunityMessagingEvents::Edit {
            community_id,
            channel_id,
            message_id,
            modified,
            lines,
            nonce,
            signature,
        } => {
            let channel = match this.document.channels.get_mut(&channel_id.to_string()) {
                Some(c) => c,
                None => return Err(Error::CommunityChannelDoesntExist),
            };

            let mut message_document = channel.get_message_document(&this.ipfs, message_id).await?;

            message_document.verify()?;

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

            message_document.set_message_with_nonce(
                keypair,
                keystore.as_ref(),
                modified,
                lines,
                (!signature.is_empty() && sender.ne(&own_did)).then_some(signature),
                Some(nonce.as_slice()),
            )?;

            channel
                .update_message_document(&this.ipfs, &message_document)
                .await?;

            this.set_document().await?;

            if let Err(e) = this
                .event_broadcast
                .send(MessageEventKind::CommunityMessageEdited {
                    community_id,
                    channel_id,
                    message_id,
                })
            {
                tracing::error!(%channel_id, error = %e, "Error broadcasting event");
            }
        }
        CommunityMessagingEvents::Delete {
            community_id,
            channel_id,
            message_id,
        } => {
            let channel = match this.document.channels.get_mut(&channel_id.to_string()) {
                Some(c) => c,
                None => return Err(Error::CommunityChannelDoesntExist),
            };

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

            channel.delete_message(&this.ipfs, message_id).await?;

            this.set_document().await?;

            if let Err(e) = this
                .event_broadcast
                .send(MessageEventKind::CommunityMessageDeleted {
                    community_id,
                    channel_id,
                    message_id,
                })
            {
                tracing::warn!(%channel_id, error = %e, "Error broadcasting event");
            }
        }
        CommunityMessagingEvents::Pin {
            community_id,
            channel_id,
            member: _,
            message_id,
            state,
        } => {
            let channel = match this.document.channels.get_mut(&channel_id.to_string()) {
                Some(c) => c,
                None => return Err(Error::CommunityChannelDoesntExist),
            };

            let mut message_document = channel.get_message_document(&this.ipfs, message_id).await?;

            let event = match state {
                PinState::Pin => {
                    if message_document.pinned() {
                        return Ok(());
                    }
                    message_document.set_pin(true);
                    MessageEventKind::CommunityMessagePinned {
                        community_id,
                        channel_id,
                        message_id,
                    }
                }
                PinState::Unpin => {
                    if !message_document.pinned() {
                        return Ok(());
                    }
                    message_document.set_pin(false);
                    MessageEventKind::CommunityMessageUnpinned {
                        community_id,
                        channel_id,
                        message_id,
                    }
                }
            };

            channel
                .update_message_document(&this.ipfs, &message_document)
                .await?;

            this.set_document().await?;

            if let Err(e) = this.event_broadcast.send(event) {
                tracing::warn!(%channel_id, error = %e, "Error broadcasting event");
            }
        }
        CommunityMessagingEvents::React {
            community_id,
            channel_id,
            reactor,
            message_id,
            state,
            emoji,
        } => {
            let channel = match this.document.channels.get_mut(&channel_id.to_string()) {
                Some(c) => c,
                None => return Err(Error::CommunityChannelDoesntExist),
            };

            let mut message_document = channel.get_message_document(&this.ipfs, message_id).await?;

            match state {
                ReactionState::Add => {
                    message_document.add_reaction(&emoji, reactor.clone())?;

                    channel
                        .update_message_document(&this.ipfs, &message_document)
                        .await?;

                    this.set_document().await?;

                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::CommunityMessageReactionAdded {
                                community_id,
                                channel_id,
                                message_id,
                                did_key: reactor,
                                reaction: emoji,
                            })
                    {
                        tracing::warn!(%channel_id, error = %e, "Error broadcasting event");
                    }
                }
                ReactionState::Remove => {
                    message_document.remove_reaction(&emoji, own_did.clone())?;

                    channel
                        .update_message_document(&this.ipfs, &message_document)
                        .await?;

                    this.set_document().await?;

                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::CommunityMessageReactionRemoved {
                            community_id,
                            channel_id,
                            message_id,
                            did_key: reactor,
                            reaction: emoji,
                        },
                    ) {
                        tracing::warn!(%channel_id, error = %e, "Error broadcasting event");
                    }
                }
            }
        }
        CommunityMessagingEvents::JoinedCommunity { community_id, user } => {
            if let Err(e) = this
                .event_broadcast
                .send(MessageEventKind::CommunityJoined { community_id, user })
            {
                tracing::warn!(%community_id, error = %e, "Error broadcasting event");
            }
        }
        CommunityMessagingEvents::UpdateCommunity { community, kind } => {
            match kind {
                CommunityUpdateKind::LeaveCommunity => {
                    this.replace_document(community).await?;
                    if let Err(e) = this
                        .event_broadcast
                        .send(MessageEventKind::LeftCommunity { community_id })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::CreateCommunityInvite { invite } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::CreatedCommunityInvite {
                                community_id,
                                invite: CommunityInvite::from(invite),
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::DeleteCommunityInvite { invite_id } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::DeletedCommunityInvite {
                                community_id,
                                invite_id,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::EditCommunityInvite { invite_id } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::EditedCommunityInvite {
                                community_id,
                                invite_id,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::CreateCommunityRole { role } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::CreatedCommunityRole {
                                community_id,
                                role: CommunityRole::from(role),
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::DeleteCommunityRole { role_id } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::DeletedCommunityRole {
                                community_id,
                                role_id,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::EditCommunityRole { role_id } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::EditedCommunityRole {
                                community_id,
                                role_id,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::GrantCommunityRole { role_id, user } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::GrantedCommunityRole {
                                community_id,
                                role_id,
                                user,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::RevokeCommunityRole { role_id, user } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::RevokedCommunityRole {
                                community_id,
                                role_id,
                                user,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::CreateCommunityChannel { channel } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::CreatedCommunityChannel {
                                community_id,
                                channel: CommunityChannel::from(channel),
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::DeleteCommunityChannel { channel_id } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::DeletedCommunityChannel {
                                community_id,
                                channel_id,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::EditCommunityName { name } => {
                    this.replace_document(community).await?;
                    if let Err(e) = this
                        .event_broadcast
                        .send(MessageEventKind::EditedCommunityName { community_id, name })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::EditCommunityDescription { description } => {
                    if let Some(desc) = description.as_ref() {
                        if desc.is_empty() || desc.len() > MAX_COMMUNITY_DESCRIPTION {
                            return Err(Error::InvalidLength {
                                context: "description".into(),
                                minimum: Some(1),
                                maximum: Some(MAX_COMMUNITY_DESCRIPTION),
                                current: desc.len(),
                            });
                        }

                        if matches!(this.document.description.as_ref(), Some(current_desc) if current_desc == desc)
                        {
                            return Ok(());
                        }
                    }

                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::EditedCommunityDescription {
                                community_id,
                                description,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::EditIcon => {
                    this.replace_document(community).await?;
                    if let Err(e) = this
                        .event_broadcast
                        .send(MessageEventKind::EditedCommunityIcon { community_id })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::EditBanner => {
                    this.replace_document(community).await?;
                    if let Err(e) = this
                        .event_broadcast
                        .send(MessageEventKind::EditedCommunityBanner { community_id })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::GrantCommunityPermission {
                    permissions,
                    role_id,
                } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::GrantedCommunityPermission {
                                community_id,
                                permissions,
                                role_id,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::RevokeCommunityPermission {
                    permissions,
                    role_id,
                } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::RevokedCommunityPermission {
                                community_id,
                                permissions,
                                role_id,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::GrantCommunityPermissionForAll { permissions } => {
                    this.replace_document(community).await?;
                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::GrantedCommunityPermissionForAll {
                            community_id,
                            permissions,
                        },
                    ) {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::RevokeCommunityPermissionForAll { permissions } => {
                    this.replace_document(community).await?;
                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::RevokedCommunityPermissionForAll {
                            community_id,
                            permissions,
                        },
                    ) {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::RemoveCommunityMember { member } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::RemovedCommunityMember {
                                community_id,
                                member,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::EditCommunityChannelName { channel_id, name } => {
                    this.replace_document(community).await?;
                    if let Err(e) =
                        this.event_broadcast
                            .send(MessageEventKind::EditedCommunityChannelName {
                                community_id,
                                channel_id,
                                name,
                            })
                    {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::EditCommunityChannelDescription {
                    channel_id,
                    description,
                } => {
                    this.replace_document(community).await?;
                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::EditedCommunityChannelDescription {
                            community_id,
                            channel_id,
                            description,
                        },
                    ) {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::GrantCommunityChannelPermission {
                    channel_id,
                    permissions,
                    role_id,
                } => {
                    this.replace_document(community).await?;
                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::GrantedCommunityChannelPermission {
                            community_id,
                            channel_id,
                            permissions,
                            role_id,
                        },
                    ) {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::RevokeCommunityChannelPermission {
                    channel_id,
                    permissions,
                    role_id,
                } => {
                    this.replace_document(community).await?;
                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::RevokedCommunityChannelPermission {
                            community_id,
                            channel_id,
                            permissions,
                            role_id,
                        },
                    ) {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::GrantCommunityChannelPermissionForAll {
                    channel_id,
                    permissions,
                } => {
                    this.replace_document(community).await?;
                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::GrantedCommunityChannelPermissionForAll {
                            community_id,
                            channel_id,
                            permissions,
                        },
                    ) {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
                CommunityUpdateKind::RevokeCommunityChannelPermissionForAll {
                    channel_id,
                    permissions,
                } => {
                    this.replace_document(community).await?;
                    if let Err(e) = this.event_broadcast.send(
                        MessageEventKind::RevokedCommunityChannelPermissionForAll {
                            community_id,
                            channel_id,
                            permissions,
                        },
                    ) {
                        tracing::warn!(%community_id, error = %e, "Error broadcasting event");
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}

async fn process_request_response_event(
    this: &mut CommunityTask,
    req: Message,
) -> Result<(), Error> {
    let keypair = &this.root.keypair().clone();
    let own_did = this.identity.did_key();

    let payload = PayloadMessage::<ConversationRequestResponse>::from_bytes(&req.data)?;

    let sender = payload.sender().to_did()?;

    let event = payload.message(keypair)?;

    tracing::debug!(id=%this.community_id, ?event, "Event received");
    match event {
        ConversationRequestResponse::Request {
            conversation_id,
            kind,
        } => match kind {
            ConversationRequestKind::Key => {
                if !this.document.participants().contains(&sender) {
                    tracing::warn!(%conversation_id, %sender, "apart of conversation");
                    return Err(Error::IdentityDoesntExist);
                }

                let keystore = &mut this.keystore;

                let raw_key = match keystore.get_latest(keypair, &own_did) {
                    Ok(key) => key,
                    Err(Error::PublicKeyDoesntExist) => {
                        let key = generate::<64>().into();
                        keystore.insert(keypair, &own_did, &key)?;

                        this.set_keystore(None).await?;
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

                let payload = PayloadBuilder::new(keypair, response)
                    .add_recipient(&sender)?
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
                            .pubsub_publish(topic.clone(), bytes.clone())
                            .await
                            .is_err())
                {
                    tracing::warn!(%conversation_id, "Unable to publish to topic. Queuing event");
                    // TODO
                    this.queue_event(
                        sender.clone(),
                        QueueItem::direct(None, peer_id, topic.clone(), bytes.clone()),
                    )
                    .await;
                }
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
                if !this.document.participants().contains(&sender) {
                    return Err(Error::IdentityDoesntExist);
                }
                let keystore = &mut this.keystore;

                let raw_key = ecdh_decrypt(keypair, Some(&sender), key)?;

                keystore.insert(keypair, &sender, raw_key)?;

                this.set_keystore(None).await?;

                if let Some(list) = this.pending_key_exchange.get_mut(&sender) {
                    for (_, received) in list {
                        *received = true;
                    }
                }
            }
            _ => {
                tracing::info!(%conversation_id, "Unimplemented/Unsupported Event");
            }
        },
    }
    Ok(())
}

async fn process_pending_payload(this: &mut CommunityTask) {
    let _this = this.borrow_mut();
    let conversation_id = _this.community_id;
    if _this.pending_key_exchange.is_empty() {
        return;
    }

    let root = _this.root.clone();

    let mut processed_events: IndexSet<_> = IndexSet::new();

    _this.pending_key_exchange.retain(|did, list| {
        list.retain(|(data, received)| {
            if *received {
                processed_events.insert((did.clone(), data.clone()));
                return false;
            }
            true
        });
        !list.is_empty()
    });

    let store = _this.keystore.clone();

    for (sender, data) in processed_events {
        // Note: Conversation keystore should exist so we could expect here, however since the map for pending exchanges would have
        //       been flushed out, we can just continue on in the iteration since it would be ignored

        let event_fn = || {
            let keypair = root.keypair();
            let key = store.get_latest(keypair, &sender)?;
            let payload = PayloadMessage::<_>::from_bytes(&data)?;
            let event = payload.message_from_key(&key)?;
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

async fn process_community_event(this: &mut CommunityTask, message: Message) -> Result<(), Error> {
    let payload = PayloadMessage::<CommunityMessagingEvents>::from_bytes(&message.data)?;
    let sender = payload.sender().to_did()?;

    let key = this.community_key(Some(&sender))?;

    let event = match payload.message_from_key(&key)? {
        event @ CommunityMessagingEvents::Event { .. } => event,
        _ => return Err(Error::Other),
    };

    if let CommunityMessagingEvents::Event {
        community_id,
        channel_id: community_channel_id,
        member,
        event,
        cancelled,
    } = event
    {
        let ev = match cancelled {
            true => MessageEventKind::CommunityEventCancelled {
                community_id,
                community_channel_id,
                did_key: member,
                event,
            },
            false => MessageEventKind::CommunityEventReceived {
                community_id,
                community_channel_id,
                did_key: member,
                event,
            },
        };

        if let Err(e) = this.event_broadcast.send(ev) {
            tracing::error!(%community_id, error = %e, "error broadcasting event");
        }
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
struct QueueItem {
    m_id: Option<Uuid>,
    peer: PeerId,
    topic: String,
    data: Bytes,
    sent: bool,
}

impl QueueItem {
    pub fn direct(m_id: Option<Uuid>, peer: PeerId, topic: String, data: impl Into<Bytes>) -> Self {
        let data = data.into();
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
async fn process_queue(this: &mut CommunityTask) {
    let mut changed = false;
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

fn pubkey_or_keystore(community: &CommunityTask) -> Result<Either<DID, Keystore>, Error> {
    let keystore = Either::Right(community.keystore.clone());
    Ok(keystore)
}
