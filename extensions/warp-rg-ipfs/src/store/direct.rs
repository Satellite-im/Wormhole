use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::Utc;
use futures::{Stream, StreamExt};
use ipfs::libp2p::swarm::dial_opts::DialOpts;
use ipfs::{Ipfs, IpfsPath, IpfsTypes, PeerId, SubscriptionStream};

use async_broadcast::{self, Receiver as BroadcastReceiver, Sender as BroadcastSender};
use libipld::serde::{from_ipld, to_ipld};
use libipld::Cid;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use warp::constellation::{Constellation, ConstellationProgressStream, Progression};
use warp::crypto::DID;
use warp::error::Error;
use warp::logging::tracing::log::{error, trace};
use warp::logging::tracing::warn;
use warp::multipass::MultiPass;
use warp::raygun::{
    Conversation, ConversationType, EmbedState, Location, Message, MessageEvent, MessageEventKind,
    MessageOptions, MessageStatus, MessageType, PinState, RayGunEventKind, Reaction, ReactionState,
};
use warp::sata::Sata;
use warp::sync::Arc;

use crate::store::connected_to_peer;
use crate::{Persistent, SpamFilter};

use super::conversation::{ConversationDocument, MessageDocument};
use super::document::{ConversationRootDocument, ToDocument};
use super::{
    did_to_libp2p_pub, topic_discovery, verify_serde_sig, ConversationEvents, MessagingEvents,
    DIRECT_BROADCAST,
};

pub struct DirectMessageStore<T: IpfsTypes> {
    // ipfs instance
    ipfs: Ipfs<T>,

    // Write handler
    path: Option<PathBuf>,

    // conversation root cid
    root_cid: Arc<tokio::sync::RwLock<Option<Cid>>>,

    // account instance
    account: Box<dyn MultiPass>,

    // filesystem instance
    filesystem: Option<Box<dyn Constellation>>,

    stream_sender: Arc<tokio::sync::RwLock<HashMap<Uuid, BroadcastSender<MessageEventKind>>>>,

    stream_receiver: Arc<tokio::sync::RwLock<HashMap<Uuid, BroadcastReceiver<MessageEventKind>>>>,

    stream_task: Arc<tokio::sync::RwLock<HashMap<Uuid, tokio::task::JoinHandle<()>>>>,

    // Queue
    queue: Arc<tokio::sync::RwLock<HashMap<DID, Vec<Queue>>>>,

    // DID
    did: Arc<DID>,

    // Event
    event: BroadcastSender<RayGunEventKind>,

    spam_filter: Arc<Option<SpamFilter>>,

    store_decrypted: Arc<AtomicBool>,

    allowed_unsigned_message: Arc<AtomicBool>,

    with_friends: Arc<AtomicBool>,
}

impl<T: IpfsTypes> Clone for DirectMessageStore<T> {
    fn clone(&self) -> Self {
        Self {
            ipfs: self.ipfs.clone(),
            path: self.path.clone(),
            stream_sender: self.stream_sender.clone(),
            stream_receiver: self.stream_receiver.clone(),
            root_cid: self.root_cid.clone(),
            account: self.account.clone(),
            filesystem: self.filesystem.clone(),
            stream_task: self.stream_task.clone(),
            queue: self.queue.clone(),
            did: self.did.clone(),
            event: self.event.clone(),
            spam_filter: self.spam_filter.clone(),
            with_friends: self.with_friends.clone(),
            allowed_unsigned_message: self.allowed_unsigned_message.clone(),
            store_decrypted: self.store_decrypted.clone(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
impl<T: IpfsTypes> DirectMessageStore<T> {
    pub async fn new(
        ipfs: Ipfs<T>,
        path: Option<PathBuf>,
        account: Box<dyn MultiPass>,
        filesystem: Option<Box<dyn Constellation>>,
        discovery: bool,
        interval_ms: u64,
        event: BroadcastSender<RayGunEventKind>,
        (check_spam, store_decrypted, allowed_unsigned_message, with_friends): (
            bool,
            bool,
            bool,
            bool,
        ),
    ) -> anyhow::Result<Self> {
        let path = match std::any::TypeId::of::<T>() == std::any::TypeId::of::<Persistent>() {
            true => path,
            false => None,
        };

        if let Some(path) = path.as_ref() {
            if !path.exists() {
                tokio::fs::create_dir_all(path).await?;
            }
        }
        // let direct_conversation = Arc::new(Default::default());
        let queue = Arc::new(Default::default());
        let root_cid = Arc::new(Default::default());
        let did = Arc::new(account.decrypt_private_key(None)?);
        let spam_filter = Arc::new(check_spam.then_some(SpamFilter::default()?));
        let stream_task = Arc::new(Default::default());
        let store_decrypted = Arc::new(AtomicBool::new(store_decrypted));
        let allowed_unsigned_message = Arc::new(AtomicBool::new(allowed_unsigned_message));
        let with_friends = Arc::new(AtomicBool::new(with_friends));
        let stream_sender = Arc::new(Default::default());
        let stream_receiver = Arc::new(Default::default());

        let mut store = Self {
            path,
            ipfs,
            stream_sender,
            stream_receiver,
            stream_task,
            root_cid,
            account,
            filesystem,
            queue,
            did,
            event,
            spam_filter,
            store_decrypted,
            allowed_unsigned_message,
            with_friends,
        };

        if let Err(_e) = store.new_cid().await {}

        if let Err(_e) = store.load_cid().await {}
        if let Err(_e) = store.load_queue().await {}

        if let Ok(list) = store.list_conversations().await {
            for conversation in list {
                let conversation = ConversationDocument::from(conversation);
                let stream = match store.ipfs.pubsub_subscribe(conversation.topic()).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Error subscribing to conversation: {e}");
                        continue;
                    }
                };
                let (tx, rx) = async_broadcast::broadcast(1024);
                store
                    .stream_sender
                    .write()
                    .await
                    .insert(conversation.id(), tx);
                store
                    .stream_receiver
                    .write()
                    .await
                    .insert(conversation.id(), rx);

                store.start_task(conversation.id(), stream).await;
            }
        }

        if discovery {
            let ipfs = store.ipfs.clone();
            tokio::spawn(async {
                if let Err(e) = topic_discovery(ipfs, DIRECT_BROADCAST).await {
                    error!("Unable to perform topic discovery: {e}");
                }
            });
        }

        let stream = store.ipfs.pubsub_subscribe(DIRECT_BROADCAST.into()).await?;

        tokio::spawn({
            let mut store = store.clone();
            async move {
                let did = &*(store.did.clone());
                futures::pin_mut!(stream);
                let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
                loop {
                    tokio::select! {
                        message = stream.next() => {
                            if let Some(message) = message {
                                if let Ok(sata) = serde_json::from_slice::<Sata>(&message.data) {
                                    if let Ok(data) = sata.decrypt::<Vec<u8>>(did.as_ref()) {
                                        if let Ok(events) = serde_json::from_slice::<ConversationEvents>(&data) {
                                            if let Err(e) = store.process_conversation(sata, events).await {
                                                error!("Error processing conversation: {e}");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ = interval.tick() => {
                            if let Err(e) = store.process_queue().await {
                                error!("Error processing queue: {e}");
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
        tokio::task::yield_now().await;
        Ok(store)
    }

    async fn start_task(&self, conversation_id: Uuid, stream: SubscriptionStream) {
        let did = self.did.clone();

        let task = warp::async_spawn({
            let mut store = self.clone();
            async move {
                futures::pin_mut!(stream);

                while let Some(stream) = stream.next().await {
                    if let Ok(data) = serde_json::from_slice::<Sata>(&stream.data) {
                        if let Ok(data) = data.decrypt::<Vec<u8>>(&did) {
                            if let Ok(event) = serde_json::from_slice::<MessagingEvents>(&data) {
                                let conversation =
                                    match store.get_conversation(conversation_id).await {
                                        Ok(c) => c,
                                        Err(_) => continue,
                                    };

                                if let Err(e) = store
                                    .direct_message_event(
                                        conversation,
                                        &event,
                                        MessageDirection::In,
                                        Default::default(),
                                    )
                                    .await
                                {
                                    error!("Error processing message: {e}");
                                    continue;
                                }
                            }
                        }
                    }
                }
            }
        });
        self.stream_task.write().await.insert(conversation_id, task);
    }

    async fn end_task(&self, conversation_id: Uuid) {
        if let Some(task) = self.stream_task.write().await.remove(&conversation_id) {
            task.abort();
        }
    }

    async fn process_conversation(
        &mut self,
        data: Sata,
        event: ConversationEvents,
    ) -> anyhow::Result<()> {
        match event {
            ConversationEvents::NewConversation(peer) => {
                let did = &*self.did;
                trace!("New conversation event received from {peer}");
                let id = super::generate_shared_topic(did, &peer, Some("direct-conversation"))?;

                if self.exist(id).await {
                    warn!("Conversation with {id} exist");
                    return Ok(());
                }

                if let Ok(true) = self.account.is_blocked(&peer) {
                    warn!("{peer} is blocked");
                    return Ok(());
                }

                let list = [did.clone(), peer];
                let convo = ConversationDocument::new_direct(did, list)?;

                let mut root_document = self.get_root_document().await?;
                root_document
                    .conversations
                    .insert(convo.to_document(self.ipfs.clone()).await?);
                self.set_root_document(root_document).await?;

                let stream = match self.ipfs.pubsub_subscribe(convo.topic()).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Error subscribing to conversation: {e}");
                        return Ok(());
                    }
                };

                let (tx, rx) = async_broadcast::broadcast(1024);

                self.stream_sender.write().await.insert(convo.id(), tx);
                self.stream_receiver.write().await.insert(convo.id(), rx);

                self.start_task(convo.id(), stream).await;

                if let Err(e) = self
                    .event
                    .broadcast(RayGunEventKind::ConversationCreated {
                        conversation_id: convo.id(),
                    })
                    .await
                {
                    error!("Error broadcasting event: {e}");
                }
            }
            ConversationEvents::DeleteConversation(conversation_id) => {
                trace!("Delete conversation event received for {conversation_id}");
                if !self.exist(conversation_id).await {
                    anyhow::bail!("Conversation {conversation_id} doesnt exist");
                }

                let sender = match data.sender() {
                    Some(sender) => DID::from(sender),
                    None => return Ok(()),
                };

                match self.get_conversation(conversation_id).await {
                    Ok(conversation)
                        if conversation.recipients().contains(&sender)
                            && conversation.conversation_type == ConversationType::Direct =>
                    {
                        conversation
                    }
                    _ => {
                        anyhow::bail!("Conversation exist but did not match condition required");
                    }
                };

                self.end_task(conversation_id).await;

                let mut root = self.get_root_document().await?;

                let document = root
                    .remove_conversation(self.ipfs.clone(), conversation_id)
                    .await?;
                self.set_root_document(root).await?;

                self.stream_sender.write().await.remove(&conversation_id);
                self.stream_receiver.write().await.remove(&conversation_id);
                self.queue.write().await.remove(&sender);

                if self
                    .ipfs
                    .pubsub_unsubscribe(&document.topic())
                    .await
                    .is_ok()
                {
                    warn!("topic should have been unsubscribed after dropping conversation.");
                }

                if let Err(e) = self
                    .event
                    .broadcast(RayGunEventKind::ConversationDeleted { conversation_id })
                    .await
                {
                    error!("Error broadcasting event: {e}");
                }
            }
        }
        Ok(())
    }

    async fn process_queue(&self) -> anyhow::Result<()> {
        let mut list = self.queue.read().await.clone();
        for (did, items) in list.iter_mut() {
            if let Ok(crate::store::PeerConnectionType::Connected) =
                connected_to_peer(self.ipfs.clone(), did.clone()).await
            {
                for item in items.iter_mut() {
                    let Queue::Direct {
                        peer,
                        topic,
                        data,
                        sent,
                        ..
                    } = item;
                    if !*sent {
                        if let Ok(peers) = self.ipfs.pubsub_peers(Some(topic.clone())).await {
                            //TODO: Check peer against conversation to see if they are connected
                            if peers.contains(peer) {
                                let bytes = match serde_json::to_vec(&data) {
                                    Ok(bytes) => bytes,
                                    Err(e) => {
                                        error!("Error serializing data to bytes: {e}");
                                        continue;
                                    }
                                };

                                if let Err(e) = self.ipfs.pubsub_publish(topic.clone(), bytes).await
                                {
                                    error!("Error publishing to topic: {e}");
                                    break;
                                }

                                *sent = true;
                            }
                        }
                    }
                    self.queue
                        .write()
                        .await
                        .entry(did.clone())
                        .or_default()
                        .retain(|queue| {
                            let Queue::Direct {
                                sent: inner_sent,
                                topic: inner_topic,
                                ..
                            } = queue;

                            if inner_topic.eq(&*topic) && *sent != *inner_sent {
                                return false;
                            }
                            true
                        });
                    self.save_queue().await;
                }
            }
        }
        Ok(())
    }
}

impl<T: IpfsTypes> DirectMessageStore<T> {
    pub async fn save_cid(&mut self, cid: Cid) -> Result<(), Error> {
        *self.root_cid.write().await = Some(cid);
        if let Some(path) = self.path.as_ref() {
            let cid = cid.to_string();
            tokio::fs::write(path.join(".conversation_id"), cid).await?;
        }
        Ok(())
    }

    #[allow(clippy::clone_on_copy)]
    pub async fn get_cid(&self) -> Result<Cid, Error> {
        (self.root_cid.read().await.clone()).ok_or(Error::Other)
    }

    pub async fn new_cid(&mut self) -> Result<(), Error> {
        if let Some(path) = self.path.as_ref() {
            if path.join(".conversation_id").is_file() {
                return Ok(());
            }
        }

        let new_root = ConversationRootDocument::new((*self.did).clone());
        self.set_root_document(new_root).await?;
        Ok(())
    }

    pub async fn load_cid(&self) -> Result<(), Error> {
        if let Some(path) = self.path.as_ref() {
            if let Ok(cid_str) = tokio::fs::read(path.join(".conversation_id"))
                .await
                .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
            {
                *self.root_cid.write().await = cid_str.parse().ok()
            }
        }
        Ok(())
    }

    pub async fn get_root_document(&self) -> Result<ConversationRootDocument, Error> {
        let root_cid = self.get_cid().await?;
        let path = IpfsPath::from(root_cid);
        self.get_dag(path, None).await
    }

    pub async fn set_root_document(
        &mut self,
        document: ConversationRootDocument,
    ) -> Result<(), Error> {
        let old_cid = self.get_cid().await.ok();

        let root_cid = self.put_dag(document).await?;

        self.ipfs.insert_pin(&root_cid, false).await?;
        self.save_cid(root_cid).await?;
        if let Some(old_cid) = old_cid {
            if self.ipfs.is_pinned(&old_cid).await? {
                self.ipfs.remove_pin(&old_cid, false).await?;
            }
            if let Err(_e) = self.ipfs.remove_block(old_cid).await {
                error!("Error removing root block: {_e}");
            }
        }

        Ok(())
    }

    pub async fn get_dag<D: DeserializeOwned>(
        &self,
        path: IpfsPath,
        timeout: Option<Duration>,
    ) -> Result<D, Error> {
        let timeout = timeout.unwrap_or(std::time::Duration::from_secs(30));
        let identity = match tokio::time::timeout(timeout, self.ipfs.get_dag(path)).await {
            Ok(Ok(ipld)) => from_ipld::<D>(ipld).map_err(anyhow::Error::from)?,
            Ok(Err(e)) => return Err(Error::Any(e)),
            Err(e) => return Err(Error::from(anyhow::anyhow!("Timeout at {e}"))),
        };
        Ok(identity)
    }

    pub async fn put_dag<S: Serialize>(&self, data: S) -> Result<Cid, Error> {
        let ipld = to_ipld(data).map_err(anyhow::Error::from)?;
        let cid = self.ipfs.put_dag(ipld).await?;
        Ok(cid)
    }

    pub async fn create_conversation(&mut self, did_key: &DID) -> Result<Conversation, Error> {
        if self.with_friends.load(Ordering::SeqCst) {
            self.account.has_friend(did_key)?;
        }

        if let Ok(true) = self.account.is_blocked(did_key) {
            return Err(Error::PublicKeyIsBlocked);
        }

        let own_did = &*(self.did.clone());

        if did_key == own_did {
            return Err(Error::CannotCreateConversation);
        }

        if let Some(conversation) = self
            .list_conversations()
            .await
            .unwrap_or_default()
            .iter()
            .find(|conversation| {
                conversation.conversation_type() == ConversationType::Direct
                    && conversation.recipients().contains(did_key)
                    && conversation.recipients().contains(own_did)
            })
            .cloned()
        {
            return Err(Error::ConversationExist { conversation });
        }

        //Temporary limit
        if self.list_conversations().await.unwrap_or_default().len() >= 32 {
            return Err(Error::ConversationLimitReached);
        }

        tokio::spawn({
            let account = self.account.clone();
            let did = did_key.clone();
            async move {
                if let Ok(list) = account.get_identity(did.into()) {
                    if list.is_empty() {
                        warn!("Unable to find identity. Creating conversation anyway");
                    }
                }
            }
        });

        let conversation =
            ConversationDocument::new_direct(own_did, [own_did.clone(), did_key.clone()])?;

        let convo_id = conversation.id();
        let topic = conversation.topic();

        let stream = self.ipfs.pubsub_subscribe(topic).await?;

        let mut root_document = self.get_root_document().await?;
        root_document
            .conversations
            .insert(conversation.to_document(self.ipfs.clone()).await?);

        self.set_root_document(root_document).await?;

        let (tx, rx) = async_broadcast::broadcast(1024);

        self.stream_sender
            .write()
            .await
            .insert(conversation.id(), tx);
        self.stream_receiver
            .write()
            .await
            .insert(conversation.id(), rx);

        self.start_task(conversation.id(), stream).await;

        let peers = self
            .ipfs
            .pubsub_peers(Some(DIRECT_BROADCAST.into()))
            .await?;

        let peer_id = did_to_libp2p_pub(did_key)?.to_peer_id();

        let mut data = Sata::default();
        data.add_recipient(did_key.as_ref())?;

        let data = data.encrypt(
            libipld::IpldCodec::DagJson,
            own_did.as_ref(),
            warp::sata::Kind::Reference,
            serde_json::to_vec(&ConversationEvents::NewConversation(own_did.clone()))?,
        )?;

        match peers.contains(&peer_id) {
            true => {
                let bytes = serde_json::to_vec(&data)?;
                if let Err(_e) = self
                    .ipfs
                    .pubsub_publish(DIRECT_BROADCAST.into(), bytes)
                    .await
                {
                    warn!("Unable to publish to topic. Queuing event");
                    if let Err(e) = self
                        .queue_event(
                            did_key.clone(),
                            Queue::direct(convo_id, None, peer_id, DIRECT_BROADCAST.into(), data),
                        )
                        .await
                    {
                        error!("Error submitting event to queue: {e}");
                    }
                }
            }
            false => {
                if let Err(e) = self
                    .queue_event(
                        did_key.clone(),
                        Queue::direct(convo_id, None, peer_id, DIRECT_BROADCAST.into(), data),
                    )
                    .await
                {
                    error!("Error submitting event to queue: {e}");
                }
            }
        };

        Ok(Conversation::from(&conversation))
    }

    pub async fn delete_conversation(
        &mut self,
        conversation_id: Uuid,
        broadcast: bool,
    ) -> Result<ConversationDocument, Error> {
        let mut root = self.get_root_document().await?;

        let document_type = root
            .remove_conversation(self.ipfs.clone(), conversation_id)
            .await?;
        self.set_root_document(root).await?;
        // conversation.end_task();
        if broadcast {
            let recipients = document_type.recipients();

            let own_did = &*self.did;

            let recipient = recipients
                .iter()
                .filter(|did| own_did.ne(did))
                .last()
                .ok_or(Error::PublicKeyInvalid)?;

            let peer_id = did_to_libp2p_pub(recipient)?.to_peer_id();

            let mut data = Sata::default();
            data.add_recipient(recipient.as_ref())?;

            let data = data.encrypt(
                libipld::IpldCodec::DagJson,
                own_did.as_ref(),
                warp::sata::Kind::Reference,
                serde_json::to_vec(&ConversationEvents::DeleteConversation(document_type.id()))?,
            )?;

            let peers = self
                .ipfs
                .pubsub_peers(Some(DIRECT_BROADCAST.into()))
                .await?;

            match peers.contains(&peer_id) {
                true => {
                    let bytes = serde_json::to_vec(&data)?;
                    if let Err(e) = self
                        .ipfs
                        .pubsub_publish(DIRECT_BROADCAST.into(), bytes)
                        .await
                    {
                        warn!("Unable to publish to topic: {e}. Queuing event");
                        //Note: If the error is related to peer not available then we should push this to queue but if
                        //      its due to the message limit being reached we should probably break up the message to fix into
                        //      "max_transmit_size" within rust-libp2p gossipsub
                        //      For now we will queue the message if we hit an error
                        if let Err(e) = self
                            .queue_event(
                                recipient.clone(),
                                Queue::direct(
                                    document_type.id(),
                                    None,
                                    peer_id,
                                    DIRECT_BROADCAST.into(),
                                    data,
                                ),
                            )
                            .await
                        {
                            error!("Error submitting event to queue: {e}");
                        }
                    }
                }
                false => {
                    if let Err(e) = self
                        .queue_event(
                            recipient.clone(),
                            Queue::direct(
                                document_type.id(),
                                None,
                                peer_id,
                                DIRECT_BROADCAST.into(),
                                data,
                            ),
                        )
                        .await
                    {
                        error!("Error submitting event to queue: {e}");
                    }
                }
            };
        }

        if let Err(e) = self
            .event
            .broadcast(RayGunEventKind::ConversationDeleted {
                conversation_id: document_type.id(),
            })
            .await
        {
            error!("Error broadcasting event: {e}");
        }
        Ok(document_type)
    }

    pub async fn list_conversations(&self) -> Result<Vec<Conversation>, Error> {
        let root = self.get_root_document().await?;
        let list = root.list_conversations(self.ipfs.clone()).await?;
        Ok(list.iter().map(|document| document.into()).collect())
    }

    pub async fn messages_count(&self, conversation: Uuid) -> Result<usize, Error> {
        let document = self.get_root_document().await?;
        let conversation = document
            .get_conversation(self.ipfs.clone(), conversation)
            .await?;
        Ok(conversation.messages.len())
    }

    pub async fn get_message(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<Message, Error> {
        let conversation = self.get_conversation(conversation_id).await?;
        conversation
            .get_message(self.ipfs.clone(), self.did.clone(), message_id)
            .await
    }

    pub async fn message_status(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageStatus, Error> {
        self.get_message(conversation_id, message_id).await?;

        let conversation = self.get_conversation(conversation_id).await?;

        let own_did = &*self.did;

        let list = conversation
            .recipients()
            .iter()
            .filter(|did| own_did.ne(did))
            .cloned()
            .collect::<Vec<_>>();

        for peer in list {
            if let Entry::Occupied(entry) = self.queue.read().await.clone().entry(peer) {
                for item in entry.get() {
                    let Queue::Direct { id, m_id, .. } = item;
                    if conversation.id() == *id {
                        if let Some(m_id) = m_id {
                            if message_id == *m_id {
                                return Ok(MessageStatus::NotSent);
                            }
                        }
                    }
                }
            }
        }

        //Not a guarantee that it been sent but for now since the message exist locally and not marked in queue, we will assume it have been sent
        Ok(MessageStatus::Sent)
    }

    pub async fn get_messages(
        &self,
        conversation: Uuid,
        opt: MessageOptions,
    ) -> Result<Vec<Message>, Error> {
        let conversation = self.get_conversation(conversation).await?;
        conversation
            .get_messages(self.ipfs.clone(), self.did.clone(), opt)
            .await
    }

    pub async fn exist(&self, conversation: Uuid) -> bool {
        if let Ok(document) = self.get_root_document().await {
            return document
                .get_conversation(self.ipfs.clone(), conversation)
                .await
                .is_ok();
        }
        false
    }

    pub async fn get_conversation(
        &self,
        conversation_id: Uuid,
    ) -> Result<ConversationDocument, Error> {
        let root = self.get_root_document().await?;

        root.get_conversation(self.ipfs.clone(), conversation_id)
            .await
    }

    pub async fn get_conversation_sender(
        &self,
        conversation_id: Uuid,
    ) -> Result<BroadcastSender<MessageEventKind>, Error> {
        let tx = self
            .stream_sender
            .read()
            .await
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?
            .clone();
        Ok(tx)
    }

    pub async fn get_conversation_receiver(
        &self,
        conversation_id: Uuid,
    ) -> Result<BroadcastReceiver<MessageEventKind>, Error> {
        let tx = self
            .stream_receiver
            .read()
            .await
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?
            .clone();
        Ok(tx)
    }

    pub async fn get_conversation_stream(
        &self,
        conversation_id: Uuid,
    ) -> Result<impl Stream<Item = MessageEventKind>, Error> {
        let mut rx = self.get_conversation_receiver(conversation_id).await?;

        Ok(async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(event) => yield event,
                    Err(async_broadcast::RecvError::Closed) => break,
                    Err(_) => {}
                };
            }
        })
    }

    pub async fn send_message(
        &mut self,
        conversation_id: Uuid,
        messages: Vec<String>,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;

        if messages.is_empty() {
            return Err(Error::EmptyMessage);
        }

        let lines_value_length: usize = messages
            .iter()
            .filter(|s| !s.is_empty())
            .map(|s| s.trim())
            .map(|s| s.chars().count())
            .sum();

        if lines_value_length == 0 || lines_value_length > 4096 {
            return Err(Error::InvalidLength {
                context: "message".into(),
                current: lines_value_length,
                minimum: Some(1),
                maximum: Some(4096),
            });
        }

        let own_did = &*self.did.clone();

        let mut message = Message::default();
        message.set_conversation_id(conversation.id());
        message.set_sender(own_did.clone());
        message.set_value(messages.clone());

        let construct = vec![
            message.id().into_bytes().to_vec(),
            message.conversation_id().into_bytes().to_vec(),
            own_did.to_string().as_bytes().to_vec(),
            message
                .value()
                .iter()
                .map(|s| s.as_bytes())
                .collect::<Vec<_>>()
                .concat(),
        ]
        .concat();

        let signature = super::sign_serde(own_did, &construct)?;
        message.set_signature(Some(signature));

        let message_id = message.id();

        let event = MessagingEvents::New(message);

        self.direct_message_event(
            conversation,
            &event,
            MessageDirection::Out,
            Default::default(),
        )
        .await?;

        self.send_raw_event(conversation_id, Some(message_id), event, true)
            .await
    }

    pub async fn edit_message(
        &mut self,
        conversation_id: Uuid,
        message_id: Uuid,
        messages: Vec<String>,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;

        if messages.is_empty() {
            return Err(Error::EmptyMessage);
        }

        let lines_value_length: usize = messages
            .iter()
            .filter(|s| !s.is_empty())
            .map(|s| s.trim())
            .map(|s| s.chars().count())
            .sum();

        if lines_value_length == 0 || lines_value_length > 4096 {
            return Err(Error::InvalidLength {
                context: "message".into(),
                current: lines_value_length,
                minimum: Some(1),
                maximum: Some(4096),
            });
        }

        let own_did = &*self.did.clone();

        let construct = vec![
            message_id.into_bytes().to_vec(),
            conversation.id().into_bytes().to_vec(),
            own_did.to_string().as_bytes().to_vec(),
            messages
                .iter()
                .map(|s| s.as_bytes())
                .collect::<Vec<_>>()
                .concat(),
        ]
        .concat();

        let signature = super::sign_serde(&self.did, &construct)?;

        let event = MessagingEvents::Edit(
            conversation.id(),
            message_id,
            Utc::now(),
            messages,
            signature,
        );

        self.direct_message_event(
            conversation,
            &event,
            MessageDirection::Out,
            Default::default(),
        )
        .await?;

        self.send_raw_event(conversation_id, None, event, true)
            .await
    }

    pub async fn reply_message(
        &mut self,
        conversation_id: Uuid,
        message_id: Uuid,
        messages: Vec<String>,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;

        if messages.is_empty() {
            return Err(Error::EmptyMessage);
        }

        let lines_value_length: usize = messages
            .iter()
            .filter(|s| !s.is_empty())
            .map(|s| s.trim())
            .map(|s| s.chars().count())
            .sum();

        if lines_value_length == 0 || lines_value_length > 4096 {
            return Err(Error::InvalidLength {
                context: "message".into(),
                current: lines_value_length,
                minimum: Some(1),
                maximum: Some(4096),
            });
        }

        let own_did = &*self.did;

        let mut message = Message::default();
        message.set_conversation_id(conversation.id());
        message.set_sender(own_did.clone());
        message.set_value(messages);
        message.set_replied(Some(message_id));

        let construct = vec![
            message.id().into_bytes().to_vec(),
            message.conversation_id().into_bytes().to_vec(),
            own_did.to_string().as_bytes().to_vec(),
            message
                .value()
                .iter()
                .map(|s| s.as_bytes())
                .collect::<Vec<_>>()
                .concat(),
        ]
        .concat();

        let signature = super::sign_serde(own_did, &construct)?;
        message.set_signature(Some(signature));

        let event = MessagingEvents::New(message);
        self.direct_message_event(
            conversation,
            &event,
            MessageDirection::Out,
            Default::default(),
        )
        .await?;

        self.send_raw_event(conversation_id, None, event, true)
            .await
    }

    pub async fn delete_message(
        &mut self,
        conversation_id: Uuid,
        message_id: Uuid,
        broadcast: bool,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;

        let event = MessagingEvents::Delete(conversation.id(), message_id);
        self.direct_message_event(
            conversation,
            &event,
            MessageDirection::Out,
            Default::default(),
        )
        .await?;

        if broadcast {
            self.send_raw_event(conversation_id, None, event, true)
                .await?;
        }

        Ok(())
    }

    pub async fn pin_message(
        &mut self,
        conversation_id: Uuid,
        message_id: Uuid,
        state: PinState,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;

        let own_did = &*self.did;

        let event = MessagingEvents::Pin(conversation.id(), own_did.clone(), message_id, state);
        self.direct_message_event(
            conversation,
            &event,
            MessageDirection::Out,
            Default::default(),
        )
        .await?;

        self.send_raw_event(conversation_id, None, event, true)
            .await
    }

    pub async fn embeds(
        &mut self,
        _conversation: Uuid,
        _message_id: Uuid,
        _state: EmbedState,
    ) -> Result<(), Error> {
        warn!("Embed function is unavailable");
        Err(Error::Unimplemented)
    }

    pub async fn react(
        &mut self,
        conversation_id: Uuid,
        message_id: Uuid,
        state: ReactionState,
        emoji: String,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;

        let own_did = &*self.did;

        let event =
            MessagingEvents::React(conversation.id(), own_did.clone(), message_id, state, emoji);

        self.direct_message_event(
            conversation,
            &event,
            MessageDirection::Out,
            Default::default(),
        )
        .await?;

        self.send_raw_event(conversation_id, None, event, true)
            .await
    }

    #[allow(clippy::await_holding_lock)]
    //TODO: Return a vector of streams for events of progression for uploading (possibly passing it through to raygun events)
    pub async fn attach(
        &mut self,
        conversation_id: Uuid,
        location: Location,
        files: Vec<PathBuf>,
        messages: Vec<String>,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;

        //TODO: Send directly if constellation isnt present
        //      this will require uploading to ipfs directly from here
        //      or setting up a seperate stream channel related to
        //      the subscribed topic possibly as a configuration option
        let mut constellation = self
            .filesystem
            .clone()
            .ok_or(Error::ConstellationExtensionUnavailable)?;

        let files = files
            .iter()
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();

        if files.is_empty() {
            return Err(Error::InvalidMessage);
        }

        let mut attachments = vec![];

        for file in files {
            let file = match location {
                Location::Constellation => {
                    let path = file.display().to_string();
                    match constellation
                        .root_directory()
                        .get_item_by_path(&path)
                        .and_then(|item| item.get_file())
                        .ok()
                    {
                        Some(f) => f,
                        None => continue,
                    }
                }
                Location::Disk => {
                    let mut filename = match file.file_name() {
                        Some(file) => file.to_string_lossy().to_string(),
                        None => continue,
                    };

                    let original = filename.clone();

                    let current_directory = constellation.current_directory()?;

                    let mut interval = 0;
                    let skip;
                    loop {
                        if current_directory.has_item(&filename) {
                            if interval >= 20 {
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
                        continue;
                    }

                    let file = tokio::fs::File::open(&file).await?;

                    let size = file.metadata().await?.len() as usize;

                    let stream = ReaderStream::new(file)
                        .filter_map(|x| async { x.ok() })
                        .map(|x| x.into());

                    let mut progress = match constellation
                        .put_stream(&filename, Some(size), stream.boxed())
                        .await
                    {
                        Ok(stream) => stream,
                        Err(e) => {
                            error!("Error uploading {filename}: {e}");
                            continue;
                        }
                    };

                    let mut complete = false;

                    while let Some(progress) = progress.next().await {
                        if let Progression::ProgressComplete { .. } = progress {
                            complete = true;
                            break;
                        }
                    }

                    if !complete {
                        continue;
                    }

                    //Note: If this fails this there might be a possible race condition
                    match current_directory
                        .get_item(&filename)
                        .and_then(|item| item.get_file())
                    {
                        Ok(file) => file,
                        Err(_) => continue,
                    }
                }
            };

            // We reconstruct it to avoid out any possible metadata that was apart of the `File` structure
            let new_file = warp::constellation::file::File::new(&file.name());
            new_file.set_size(file.size());
            new_file.set_hash(file.hash());
            new_file.set_reference(&file.reference().unwrap_or_default());
            attachments.push(new_file);
        }

        let own_did = &*self.did.clone();

        let mut message = Message::default();
        message.set_message_type(MessageType::Attachment);
        message.set_conversation_id(conversation.id());
        message.set_sender(own_did.clone());
        message.set_attachment(attachments);
        message.set_value(messages.clone());

        let construct = vec![
            message.id().into_bytes().to_vec(),
            message.conversation_id().into_bytes().to_vec(),
            own_did.to_string().as_bytes().to_vec(),
            message
                .value()
                .iter()
                .map(|s| s.as_bytes())
                .collect::<Vec<_>>()
                .concat(),
        ]
        .concat();

        let signature = super::sign_serde(own_did, &construct)?;
        message.set_signature(Some(signature));

        let event = MessagingEvents::New(message);

        self.direct_message_event(
            conversation,
            &event,
            MessageDirection::Out,
            Default::default(),
        )
        .await?;

        self.send_raw_event(conversation_id, None, event, true)
            .await
    }

    #[allow(clippy::await_holding_lock)]
    pub async fn download(
        &self,
        conversation: Uuid,
        message_id: Uuid,
        file: &str,
        path: PathBuf,
        _: bool,
    ) -> Result<ConstellationProgressStream, Error> {
        let constellation = self
            .filesystem
            .clone()
            .ok_or(Error::ConstellationExtensionUnavailable)?;

        if constellation.id() != "warp-fs-ipfs" {
            //Note: Temporary for now; Will get lifted in the future
            return Err(Error::Unimplemented);
        }

        let message = self.get_message(conversation, message_id).await?;

        if message.message_type() != MessageType::Attachment {
            return Err(Error::InvalidMessage);
        }

        let attachment = message
            .attachments()
            .iter()
            .find(|attachment| attachment.name() == file)
            .cloned()
            .ok_or(Error::FileNotFound)?;

        let root = constellation.root_directory();
        if !root.has_item(&attachment.name()) {
            root.add_file(attachment.clone())?;
        }

        let ipfs = self.ipfs.clone();
        let constellation = constellation.clone();
        let own_did = self.did.clone();

        let progress_stream = async_stream::stream! {
                yield Progression::CurrentProgress {
                    name: attachment.name(),
                    current: 0,
                    total: Some(attachment.size()),
                };

                let did = message.sender();
                if !did.eq(&own_did) {
                    if let Ok(peer_id) = did_to_libp2p_pub(&did).map(|pk| pk.to_peer_id()) {
                        match ipfs.find_peer_info(peer_id).await {
                            Ok(info) => {
                                //This is done to insure we can successfully exchange blocks
                                let opt = DialOpts::peer_id(peer_id)
                                    .addresses(info.listen_addrs)
                                    .extend_addresses_through_behaviour()
                                    .build();

                                if let Err(e) = ipfs.dial(opt).await {
                                    error!("Error dialing peer: {e}");
                                }
                            }
                            _ => {
                                warn!("Sender not found or is not connected");
                            }
                        };
                    }
                }

                let mut file = match tokio::fs::File::create(&path).await {
                    Ok(file) => file,
                    Err(e) => {
                        error!("Error creating file: {e}");
                        yield Progression::ProgressFailed {
                                    name: attachment.name(),
                                    last_size: None,
                                    error: Some(e.to_string()),
                        };
                        return;
                    }
                };

                let stream = match constellation.get_stream(&attachment.name()).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Error creating stream: {e}");
                        yield Progression::ProgressFailed {
                                    name: attachment.name(),
                                    last_size: None,
                                    error: Some(e.to_string()),
                        };
                        return;
                    }
                };

                let mut written = 0;
                let mut failed = false;
                for await res in stream  {
                    match res {
                        Ok(bytes) => match file.write_all(&bytes).await {
                            Ok(_) => {
                                written += bytes.len();
                                yield Progression::CurrentProgress {
                                    name: attachment.name(),
                                    current: written,
                                    total: Some(attachment.size()),
                                };
                            }
                            Err(e) => {
                                error!("Error writing to disk: {e}");
                                yield Progression::ProgressFailed {
                                    name: attachment.name(),
                                    last_size: Some(written),
                                    error: Some(e.to_string()),
                                };
                                failed = true;
                                break;
                            }
                        },
                        Err(e) => {
                            error!("Error reading from stream: {e}");
                            yield Progression::ProgressFailed {
                                    name: attachment.name(),
                                    last_size: Some(written),
                                    error: Some(e.to_string()),
                            };
                            failed = true;
                            break;
                        }
                    }
                }

                if failed {
                    if let Err(e) = tokio::fs::remove_file(&path).await {
                        error!("Error removing file: {e}");
                    }
                }

                if !failed {
                    if let Err(e) = file.flush().await {
                        error!("Error flushing stream: {e}");
                    }
                    yield Progression::ProgressComplete {
                        name: attachment.name(),
                        total: Some(written),
                    };
                }
        };

        Ok(ConstellationProgressStream(progress_stream.boxed()))
    }

    pub async fn send_event(
        &mut self,
        conversation_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;
        let own_did = &*self.did;

        let event = MessagingEvents::Event(conversation.id(), own_did.clone(), event, false);

        self.direct_message_event(
            conversation,
            &event,
            MessageDirection::Out,
            Default::default(),
        )
        .await?;

        self.send_raw_event(conversation_id, None, event, false)
            .await
    }

    pub async fn cancel_event(
        &mut self,
        conversation_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;
        let own_did = &*self.did;

        let event = MessagingEvents::Event(conversation.id(), own_did.clone(), event, true);

        self.direct_message_event(
            conversation,
            &event,
            MessageDirection::Out,
            Default::default(),
        )
        .await?;

        self.send_raw_event(conversation_id, None, event, false)
            .await
    }

    pub async fn send_raw_event<S: Serialize + Send + Sync>(
        &mut self,
        conversation: Uuid,
        message_id: Option<Uuid>,
        event: S,
        queue: bool,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation).await?;

        let recipients = conversation.recipients();

        let own_did = &*self.did;

        let recipient = recipients
            .iter()
            .filter(|did| own_did.ne(did))
            .last()
            .ok_or(Error::PublicKeyInvalid)?;

        let mut data = Sata::default();
        data.add_recipient(recipient.as_ref())?;

        let payload = data.encrypt(
            libipld::IpldCodec::DagJson,
            own_did.as_ref(),
            warp::sata::Kind::Reference,
            serde_json::to_vec(&event)?,
        )?;

        let bytes = serde_json::to_vec(&payload)?;

        let peers = self.ipfs.pubsub_peers(Some(conversation.topic())).await?;

        let peer_id = did_to_libp2p_pub(recipient)?.to_peer_id();

        match peers.contains(&peer_id) {
            true => {
                if let Err(e) = self.ipfs.pubsub_publish(conversation.topic(), bytes).await {
                    if queue {
                        warn!("Unable to publish to topic due to error: {e}... Queuing event");
                        if let Err(e) = self
                            .queue_event(
                                recipient.clone(),
                                Queue::direct(
                                    conversation.id(),
                                    message_id,
                                    peer_id,
                                    conversation.topic(),
                                    payload,
                                ),
                            )
                            .await
                        {
                            error!("Error submitting event to queue: {e}");
                        }
                    }
                }
            }
            false => {
                if queue {
                    if let Err(e) = self
                        .queue_event(
                            recipient.clone(),
                            Queue::direct(
                                conversation.id(),
                                message_id,
                                peer_id,
                                conversation.topic(),
                                payload,
                            ),
                        )
                        .await
                    {
                        error!("Error submitting event to queue: {e}");
                    }
                }
            }
        };

        Ok(())
    }

    async fn queue_event(&mut self, did: DID, queue: Queue) -> Result<(), Error> {
        self.queue.write().await.entry(did).or_default().push(queue);
        self.save_queue().await;

        Ok(())
    }

    async fn save_queue(&self) {
        if let Some(path) = self.path.as_ref() {
            let bytes = match serde_json::to_vec(&*self.queue.read().await) {
                Ok(bytes) => bytes,
                Err(e) => {
                    error!("Error serializing queue list into bytes: {e}");
                    return;
                }
            };

            if let Err(e) = tokio::fs::write(path.join(".messaging_queue"), bytes).await {
                error!("Error saving queue: {e}");
            }
        }
    }

    async fn load_queue(&self) -> anyhow::Result<()> {
        if let Some(path) = self.path.as_ref() {
            let data = tokio::fs::read(path.join(".messaging_queue")).await?;
            *self.queue.write().await = serde_json::from_slice(&data)?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn direct_message_event<'a>(
        &mut self,
        mut document: ConversationDocument,
        events: &'a MessagingEvents,
        direction: MessageDirection,
        opt: EventOpt,
    ) -> Result<bool, Error> {
        let tx = self.get_conversation_sender(document.id()).await?;
        match events.clone() {
            MessagingEvents::New(mut message) => {
                if document
                    .messages
                    .iter()
                    .any(|message_document| message_document.id == message.id())
                {
                    return Err(Error::MessageFound);
                }

                let lines_value_length: usize = message
                    .value()
                    .iter()
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.chars().count())
                    .sum();

                if lines_value_length >= 4096 {
                    return Err(Error::InvalidLength {
                        context: "message".into(),
                        current: lines_value_length,
                        minimum: Some(1),
                        maximum: Some(4096),
                    });
                }

                {
                    let signature = message.signature();
                    let sender = message.sender();
                    let construct = vec![
                        message.id().into_bytes().to_vec(),
                        message.conversation_id().into_bytes().to_vec(),
                        sender.to_string().as_bytes().to_vec(),
                        message
                            .value()
                            .iter()
                            .map(|s| s.as_bytes())
                            .collect::<Vec<_>>()
                            .concat(),
                    ]
                    .concat();
                    verify_serde_sig(sender, &construct, &signature)?;
                }
                spam_check(&mut message, self.spam_filter.clone())?;
                let conversation_id = message.conversation_id();

                if message.message_type() == MessageType::Attachment
                    && direction == MessageDirection::In
                {
                    if let Some(fs) = self.filesystem.clone() {
                        let dir = fs.root_directory();
                        for file in message.attachments() {
                            let original = file.name();
                            let mut inc = 0;
                            loop {
                                if dir.has_item(&original) {
                                    if inc >= 20 {
                                        break;
                                    }
                                    inc += 1;
                                    file.set_name(&format!("{original}-{inc}"));
                                    continue;
                                }
                                break;
                            }
                            if let Err(e) = dir.add_file(file) {
                                error!("Error adding file to constellation: {e}");
                            }
                        }
                    }
                }

                let message_id = message.id();

                let message_document = MessageDocument::new(
                    self.ipfs.clone(),
                    self.did.clone(),
                    document.recipients(),
                    message,
                )
                .await?;

                document.messages.insert(message_document);

                let mut root = self.get_root_document().await?;

                root.update_conversation(self.ipfs.clone(), conversation_id, document.clone())
                    .await?;

                self.set_root_document(root).await?;

                let event = match direction {
                    MessageDirection::In => MessageEventKind::MessageReceived {
                        conversation_id,
                        message_id,
                    },
                    MessageDirection::Out => MessageEventKind::MessageSent {
                        conversation_id,
                        message_id,
                    },
                };

                if let Err(e) = tx.broadcast(event).await {
                    error!("Error broadcasting event: {e}");
                }
            }
            MessagingEvents::Edit(convo_id, message_id, modified, val, signature) => {
                let mut message_document = document
                    .messages
                    .iter()
                    .find(|document| {
                        document.id == message_id && document.conversation_id == convo_id
                    })
                    .cloned()
                    .ok_or(Error::MessageNotFound)?;

                let mut message = message_document
                    .resolve(self.ipfs.clone(), self.did.clone())
                    .await?;

                let lines_value_length: usize = val
                    .iter()
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.chars().count())
                    .sum();

                if lines_value_length >= 4096 {
                    return Err(Error::InvalidLength {
                        context: "message".into(),
                        current: lines_value_length,
                        minimum: Some(1),
                        maximum: Some(4096),
                    });
                }

                let sender = message.sender();
                //Validate the original message
                {
                    let signature = message.signature();
                    let construct = vec![
                        message.id().into_bytes().to_vec(),
                        message.conversation_id().into_bytes().to_vec(),
                        sender.to_string().as_bytes().to_vec(),
                        message
                            .value()
                            .iter()
                            .map(|s| s.as_bytes())
                            .collect::<Vec<_>>()
                            .concat(),
                    ]
                    .concat();
                    verify_serde_sig(sender.clone(), &construct, &signature)?;
                }

                //Validate the edit message
                {
                    let construct = vec![
                        message.id().into_bytes().to_vec(),
                        message.conversation_id().into_bytes().to_vec(),
                        sender.to_string().as_bytes().to_vec(),
                        val.iter()
                            .map(|s| s.as_bytes())
                            .collect::<Vec<_>>()
                            .concat(),
                    ]
                    .concat();
                    verify_serde_sig(sender, &construct, &signature)?;
                }

                message.set_signature(Some(signature));
                *message.value_mut() = val;
                message.set_modified(modified);

                message_document
                    .update(self.ipfs.clone(), self.did.clone(), message)
                    .await?;

                document.messages.replace(message_document);

                let mut root = self.get_root_document().await?;

                root.update_conversation(self.ipfs.clone(), convo_id, document.clone())
                    .await?;

                self.set_root_document(root).await?;

                if let Err(e) = tx
                    .broadcast(MessageEventKind::MessageEdited {
                        conversation_id: convo_id,
                        message_id,
                    })
                    .await
                {
                    error!("Error broadcasting event: {e}");
                }
            }
            MessagingEvents::Delete(convo_id, message_id) => {
                let message_document = document
                    .messages
                    .iter()
                    .cloned()
                    .find(|document| {
                        document.id == message_id && document.conversation_id == convo_id
                    })
                    .ok_or(Error::MessageNotFound)?;

                if opt.keep_if_owned.load(Ordering::SeqCst) {
                    let message = message_document
                        .resolve(self.ipfs.clone(), self.did.clone())
                        .await?;
                    let signature = message.signature();
                    let sender = message.sender();
                    let construct = vec![
                        message.id().into_bytes().to_vec(),
                        message.conversation_id().into_bytes().to_vec(),
                        sender.to_string().as_bytes().to_vec(),
                        message
                            .value()
                            .iter()
                            .map(|s| s.as_bytes())
                            .collect::<Vec<_>>()
                            .concat(),
                    ]
                    .concat();
                    verify_serde_sig(sender, &construct, &signature)?;
                }

                if document.messages.remove(&message_document) {
                    let mut root = self.get_root_document().await?;

                    root.update_conversation(self.ipfs.clone(), convo_id, document.clone())
                        .await?;

                    self.set_root_document(root).await?;

                    if let Err(e) = tx
                        .broadcast(MessageEventKind::MessageDeleted {
                            conversation_id: convo_id,
                            message_id,
                        })
                        .await
                    {
                        error!("Error broadcasting event: {e}");
                    }
                }
            }
            MessagingEvents::Pin(convo_id, _, message_id, state) => {
                let mut message_document = document
                    .messages
                    .iter()
                    .find(|document| {
                        document.id == message_id && document.conversation_id == convo_id
                    })
                    .cloned()
                    .ok_or(Error::MessageNotFound)?;

                let mut message = message_document
                    .resolve(self.ipfs.clone(), self.did.clone())
                    .await?;

                let event = match state {
                    PinState::Pin => {
                        *message.pinned_mut() = true;
                        MessageEventKind::MessagePinned {
                            conversation_id: convo_id,
                            message_id,
                        }
                    }
                    PinState::Unpin => {
                        *message.pinned_mut() = false;
                        MessageEventKind::MessageUnpinned {
                            conversation_id: convo_id,
                            message_id,
                        }
                    }
                };

                message_document
                    .update(self.ipfs.clone(), self.did.clone(), message)
                    .await?;

                document.messages.replace(message_document);

                if let Err(e) = tx.broadcast(event).await {
                    error!("Error broadcasting event: {e}");
                }
            }
            MessagingEvents::React(convo_id, sender, message_id, state, emoji) => {
                let mut message_document = document
                    .messages
                    .iter()
                    .find(|document| {
                        document.id == message_id && document.conversation_id == convo_id
                    })
                    .cloned()
                    .ok_or(Error::MessageNotFound)?;

                let mut message = message_document
                    .resolve(self.ipfs.clone(), self.did.clone())
                    .await?;

                let reactions = message.reactions_mut();

                match state {
                    ReactionState::Add => {
                        let index = match reactions
                            .iter()
                            .position(|reaction| reaction.emoji().eq(&emoji))
                        {
                            Some(index) => index,
                            None => {
                                let mut reaction = Reaction::default();
                                reaction.set_emoji(&emoji);
                                reaction.set_users(vec![sender.clone()]);
                                reactions.push(reaction);
                                if let Err(e) = tx
                                    .broadcast(MessageEventKind::MessageReactionAdded {
                                        conversation_id: convo_id,
                                        message_id,
                                        did_key: sender,
                                        reaction: emoji,
                                    })
                                    .await
                                {
                                    error!("Error broadcasting event: {e}");
                                }
                                return Ok(false);
                            }
                        };

                        let reaction = reactions.get_mut(index).ok_or(Error::MessageNotFound)?;

                        reaction.users_mut().push(sender.clone());

                        message_document
                            .update(self.ipfs.clone(), self.did.clone(), message)
                            .await?;
                        document.messages.replace(message_document);
                        if let Err(e) = tx
                            .broadcast(MessageEventKind::MessageReactionAdded {
                                conversation_id: convo_id,
                                message_id,
                                did_key: sender,
                                reaction: emoji,
                            })
                            .await
                        {
                            error!("Error broadcasting event: {e}");
                        }
                    }
                    ReactionState::Remove => {
                        let index = reactions
                            .iter()
                            .position(|reaction| {
                                reaction.users().contains(&sender) && reaction.emoji().eq(&emoji)
                            })
                            .ok_or(Error::MessageNotFound)?;

                        let reaction = reactions.get_mut(index).ok_or(Error::MessageNotFound)?;

                        let user_index = reaction
                            .users()
                            .iter()
                            .position(|reaction_sender| reaction_sender.eq(&sender))
                            .ok_or(Error::MessageNotFound)?;

                        reaction.users_mut().remove(user_index);

                        if reaction.users().is_empty() {
                            //Since there is no users listed under the emoji, the reaction should be removed from the message
                            reactions.remove(index);

                            message_document
                                .update(self.ipfs.clone(), self.did.clone(), message)
                                .await?;
                            document.messages.replace(message_document);
                            if let Err(e) = tx
                                .broadcast(MessageEventKind::MessageReactionRemoved {
                                    conversation_id: convo_id,
                                    message_id,
                                    did_key: sender,
                                    reaction: emoji,
                                })
                                .await
                            {
                                error!("Error broadcasting event: {e}");
                            }
                        }
                    }
                }
            }
            MessagingEvents::Event(conversation_id, did_key, event, cancelled) => {
                if direction == MessageDirection::In {
                    let ev = match cancelled {
                        true => MessageEventKind::EventCancelled {
                            conversation_id,
                            did_key,
                            event,
                        },
                        false => MessageEventKind::EventReceived {
                            conversation_id,
                            did_key,
                            event,
                        },
                    };
                    if let Err(e) = tx.broadcast(ev).await {
                        error!("Error broadcasting event: {e}");
                    }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

#[derive(Clone, Default)]
pub struct EventOpt {
    pub keep_if_owned: Arc<AtomicBool>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MessageDirection {
    In,
    Out,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Queue {
    Direct {
        id: Uuid,
        m_id: Option<Uuid>,
        peer: PeerId,
        topic: String,
        data: Sata,
        sent: bool,
    },
}

impl Queue {
    pub fn direct(id: Uuid, m_id: Option<Uuid>, peer: PeerId, topic: String, data: Sata) -> Self {
        Queue::Direct {
            id,
            m_id,
            peer,
            topic,
            data,
            sent: false,
        }
    }
}

pub fn spam_check(message: &mut Message, filter: Arc<Option<SpamFilter>>) -> anyhow::Result<()> {
    if let Some(filter) = filter.as_ref() {
        if filter.process(&message.value().join(" "))? {
            message
                .metadata_mut()
                .insert("is_spam".to_owned(), "true".to_owned());
        }
    }
    Ok(())
}
