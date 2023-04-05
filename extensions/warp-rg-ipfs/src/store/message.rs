use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::Utc;
use futures::channel::mpsc::{unbounded, UnboundedSender};
use futures::channel::oneshot::Sender as OneshotSender;
use futures::stream::FuturesUnordered;
use futures::{SinkExt, Stream, StreamExt};
use rust_ipfs::libp2p::swarm::dial_opts::DialOpts;
use rust_ipfs::{Ipfs, PeerId, SubscriptionStream};

use libipld::Cid;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast::{self, Receiver as BroadcastReceiver, Sender as BroadcastSender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::ReadDirStream;
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use warp::constellation::{Constellation, ConstellationProgressStream, Progression};
use warp::crypto::cipher::Cipher;
use warp::crypto::{generate, DID};
use warp::error::Error;
use warp::logging::tracing::log::{error, info, trace};
use warp::logging::tracing::warn;
use warp::multipass::MultiPass;
use warp::raygun::{
    Conversation, ConversationType, EmbedState, Location, Message, MessageEvent, MessageEventKind,
    MessageOptions, MessageStatus, MessageStream, MessageType, Messages, MessagesType, PinState,
    RayGunEventKind, Reaction, ReactionState,
};
use warp::sync::Arc;

use crate::store::payload::Payload;
use crate::store::{
    connected_to_peer, ecdh_decrypt, ecdh_encrypt, sign_serde, ConversationRequestResponse,
    ConversationResponse,
};
use crate::SpamFilter;

use super::conversation::{ConversationDocument, MessageDocument};
use super::document::{GetLocalDag, ToCid};
use super::keystore::Keystore;
use super::{
    did_to_libp2p_pub, topic_discovery, verify_serde_sig, ConversationEvents, ConversationRequest,
    MessagingEvents,
};

const PERMIT_AMOUNT: usize = 1;

type ConversationSender =
    UnboundedSender<(MessagingEvents, Option<OneshotSender<Result<(), Error>>>)>;

pub struct MessageStore {
    // ipfs instance
    ipfs: Ipfs,

    // Write handler
    path: Option<PathBuf>,

    // conversation cid
    conversation_cid: Arc<tokio::sync::RwLock<HashMap<Uuid, Cid>>>,
    conversation_keystore_cid: Arc<tokio::sync::RwLock<HashMap<Uuid, Cid>>>,

    conversation_lock: Arc<tokio::sync::RwLock<HashMap<Uuid, Arc<Semaphore>>>>,

    conversation_sender: Arc<tokio::sync::RwLock<HashMap<Uuid, ConversationSender>>>,

    // account instance
    account: Box<dyn MultiPass>,

    // filesystem instance
    filesystem: Option<Box<dyn Constellation>>,

    stream_sender: Arc<tokio::sync::RwLock<HashMap<Uuid, BroadcastSender<MessageEventKind>>>>,

    stream_task: Arc<tokio::sync::RwLock<HashMap<Uuid, tokio::task::JoinHandle<()>>>>,
    stream_reqres_task: Arc<tokio::sync::RwLock<HashMap<Uuid, tokio::task::JoinHandle<()>>>>,
    stream_event_task: Arc<tokio::sync::RwLock<HashMap<Uuid, tokio::task::JoinHandle<()>>>>,

    // Queue
    queue: Arc<tokio::sync::RwLock<HashMap<DID, Vec<Queue>>>>,

    // DID
    did: Arc<DID>,

    // Event
    event: BroadcastSender<RayGunEventKind>,

    spam_filter: Arc<Option<SpamFilter>>,

    with_friends: Arc<AtomicBool>,

    attach_recipients_on_storing: Arc<AtomicBool>,

    disable_sender_event_emit: Arc<AtomicBool>,
}

impl Clone for MessageStore {
    fn clone(&self) -> Self {
        Self {
            ipfs: self.ipfs.clone(),
            path: self.path.clone(),
            stream_sender: self.stream_sender.clone(),
            conversation_keystore_cid: self.conversation_keystore_cid.clone(),
            conversation_cid: self.conversation_cid.clone(),
            conversation_sender: self.conversation_sender.clone(),
            conversation_lock: self.conversation_lock.clone(),
            account: self.account.clone(),
            filesystem: self.filesystem.clone(),
            stream_task: self.stream_task.clone(),
            stream_event_task: self.stream_event_task.clone(),
            stream_reqres_task: self.stream_reqres_task.clone(),
            queue: self.queue.clone(),
            did: self.did.clone(),
            event: self.event.clone(),
            spam_filter: self.spam_filter.clone(),
            with_friends: self.with_friends.clone(),
            attach_recipients_on_storing: self.attach_recipients_on_storing.clone(),
            disable_sender_event_emit: self.disable_sender_event_emit.clone(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
impl MessageStore {
    pub async fn new(
        ipfs: Ipfs,
        path: Option<PathBuf>,
        account: Box<dyn MultiPass>,
        filesystem: Option<Box<dyn Constellation>>,
        discovery: bool,
        interval_ms: u64,
        event: BroadcastSender<RayGunEventKind>,
        (
            check_spam,
            disable_sender_event_emit,
            with_friends,
            conversation_load_task,
            attach_recipients_on_storing,
        ): (bool, bool, bool, bool, bool),
    ) -> anyhow::Result<Self> {
        info!("Initializing MessageStore");

        if let Some(path) = path.as_ref() {
            if !path.exists() {
                tokio::fs::create_dir_all(path).await?;
            }
        }

        let queue = Arc::new(Default::default());
        let conversation_cid = Arc::new(Default::default());
        let did = Arc::new(account.decrypt_private_key(None)?);
        let spam_filter = Arc::new(check_spam.then_some(SpamFilter::default()?));
        let stream_task = Arc::new(Default::default());
        let stream_event_task = Arc::new(Default::default());
        let disable_sender_event_emit = Arc::new(AtomicBool::new(disable_sender_event_emit));
        let with_friends = Arc::new(AtomicBool::new(with_friends));
        let attach_recipients_on_storing = Arc::new(AtomicBool::new(attach_recipients_on_storing));
        let stream_sender = Arc::new(Default::default());
        let conversation_lock = Arc::new(Default::default());
        let conversation_sender = Arc::default();
        let conversation_keystore_cid = Arc::default();

        let stream_reqres_task = Arc::default();

        let store = Self {
            path,
            ipfs,
            stream_sender,
            stream_task,
            stream_event_task,
            stream_reqres_task,
            conversation_cid,
            conversation_lock,
            conversation_sender,
            account,
            filesystem,
            queue,
            did,
            event,
            spam_filter,
            disable_sender_event_emit,
            with_friends,
            attach_recipients_on_storing,
            conversation_keystore_cid,
        };

        info!("Loading existing conversations task");
        if let Err(_e) = store.load_conversations(conversation_load_task).await {}

        tokio::spawn({
            let mut store = store.clone();
            async move {
                info!("MessagingStore task created");

                tokio::spawn({
                    let store = store.clone();
                    async move {
                        info!("Loading queue");
                        // Load the queue in a separate task in case it is large
                        // Note: In the future this will not be needed once a req/res system
                        //       is implemented
                        if let Err(_e) = store.load_queue().await {}
                    }
                });

                let did = &*(store.did.clone());
                let Ok(stream) = store.ipfs.pubsub_subscribe(format!("{did}/messaging")).await else {
                    error!("Unable to create subscription stream. Terminating task");
                    //TODO: Maybe panic? 
                    return;
                };
                futures::pin_mut!(stream);
                let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
                loop {
                    tokio::select! {
                        message = stream.next() => {
                            if let Some(message) = message {
                                if let Ok(payload) = Payload::from_bytes(&message.data) {
                                    if let Ok(data) = ecdh_decrypt(&store.did, Some(payload.sender()), payload.data()) {
                                        if let Ok(events) = serde_json::from_slice::<ConversationEvents>(&data) {
                                            if let Err(e) = store.process_conversation(payload, events).await {
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
                }
            }
        });
        if discovery {
            let ipfs = store.ipfs.clone();
            let topic = format!("{}/messaging", store.did);
            tokio::spawn(async move {
                if let Err(e) = topic_discovery(ipfs, topic).await {
                    error!("Unable to perform topic discovery: {e}");
                }
            });
        }
        tokio::task::yield_now().await;
        Ok(store)
    }

    async fn start_event_task(&self, conversation_id: Uuid) {
        info!("Event Task started for {conversation_id}");
        let did = self.did.clone();
        let Ok(mut conversation) = self.get_conversation(conversation_id).await else {
            return
        };

        conversation.messages.clear();
        conversation.recipients.clear();

        let Ok(tx) = self.get_conversation_sender(conversation_id).await else {
            return
        };

        let Ok(stream) = self.ipfs.pubsub_subscribe(conversation.event_topic()).await else {
            return
        };

        let conversation_type = conversation.conversation_type;

        drop(conversation);
        let task = tokio::spawn({
            let store = self.clone();
            async move {
                futures::pin_mut!(stream);

                while let Some(stream) = stream.next().await {
                    if let Ok(payload) = Payload::from_bytes(&stream.data) {
                        let bytes = {
                            let own_did = &*did;
                            let store = store.clone();
                            async move {
                                match conversation_type {
                                    ConversationType::Direct => {
                                        let recipient = store
                                            .get_conversation(conversation_id)
                                            .await
                                            .map(|c| c.recipients())
                                            .unwrap_or_default()
                                            .iter()
                                            .filter(|did| own_did.ne(did))
                                            .cloned()
                                            .collect::<Vec<_>>()
                                            .first()
                                            .cloned()
                                            .ok_or(Error::InvalidConversation)?;
                                        ecdh_decrypt(own_did, Some(recipient), payload.data())
                                    }
                                    ConversationType::Group => {
                                        let keystore =
                                            store.conversation_keystore(conversation_id).await?;
                                        let key =
                                            keystore.get_latest(own_did, &payload.sender())?;
                                        Cipher::direct_decrypt(payload.data(), &key)
                                    }
                                }
                            }
                        };

                        if let Ok(data) = bytes.await {
                            if let Ok(MessagingEvents::Event(
                                conversation_id,
                                did_key,
                                event,
                                cancelled,
                            )) = serde_json::from_slice::<MessagingEvents>(&data)
                            {
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
                                if let Err(e) = tx.send(ev) {
                                    error!("Error broadcasting event: {e}");
                                }
                            }
                        }
                    }
                }
            }
        });
        self.stream_event_task
            .write()
            .await
            .insert(conversation_id, task);
    }

    async fn start_reqres_task(&self, conversation_id: Uuid) {
        info!("RequestResponse Task started for {conversation_id}");
        let did = self.did.clone();
        let Ok(mut conversation) = self.get_conversation(conversation_id).await else {
            return
        };

        conversation.messages.clear();
        conversation.recipients.clear();

        let Ok(stream) = self.ipfs.pubsub_subscribe(conversation.reqres_topic(&did)).await else {
            return
        };

        let task = tokio::spawn({
            let store = self.clone();
            async move {
                futures::pin_mut!(stream);

                while let Some(stream) = stream.next().await {
                    if let Ok(payload) = Payload::from_bytes(&stream.data) {
                        if let Ok(data) = ecdh_decrypt(&did, Some(payload.sender()), payload.data())
                        {
                            if let Ok(event) =
                                serde_json::from_slice::<ConversationRequestResponse>(&data)
                            {
                                match event {
                                    ConversationRequestResponse::Request(request) => {
                                        match request {
                                            ConversationRequest::Key { conversation_id } => {
                                                let mut keystore = match store
                                                    .conversation_keystore(conversation_id)
                                                    .await
                                                {
                                                    Ok(keystore) => keystore,
                                                    Err(e) => {
                                                        error!("Error obtaining keystore: {e}. Skipping");
                                                        continue;
                                                    }
                                                };

                                                let raw_key = match keystore.get_latest(&did, &did)
                                                {
                                                    Ok(key) => key,
                                                    Err(Error::PublicKeyInvalid) => {
                                                        let key = generate(64);
                                                        if let Err(e) =
                                                            keystore.insert(&did, &did, &key)
                                                        {
                                                            error!("Error inserting generated key into store: {e}");
                                                            continue;
                                                        }
                                                        if let Err(e) = store
                                                            .set_conversation_keystore(
                                                                conversation_id,
                                                                &keystore,
                                                            )
                                                            .await
                                                        {
                                                            error!("Error setting keystore: {e}");
                                                            continue;
                                                        }
                                                        key
                                                    }
                                                    Err(e) => {
                                                        error!("Error getting key from store: {e}");
                                                        continue;
                                                    }
                                                };
                                                let sender = payload.sender();
                                                let key = match ecdh_encrypt(
                                                    &did,
                                                    Some(sender.clone()),
                                                    raw_key,
                                                ) {
                                                    Ok(key) => key,
                                                    Err(e) => {
                                                        error!("Error: {e}");
                                                        continue;
                                                    }
                                                };
                                                let response =
                                                    ConversationRequestResponse::Response(
                                                        ConversationResponse::Key {
                                                            conversation_id,
                                                            key,
                                                        },
                                                    );
                                                let result = {
                                                    let did = did.clone();
                                                    let store = store.clone();
                                                    let topic = conversation.reqres_topic(&sender);
                                                    async move {
                                                        let bytes = ecdh_encrypt(
                                                            &did,
                                                            Some(sender.clone()),
                                                            serde_json::to_vec(&response)?,
                                                        )?;
                                                        let signature = sign_serde(&did, &bytes)?;

                                                        let payload =
                                                            Payload::new(&did, &bytes, &signature);

                                                        let bytes = payload.to_bytes()?;

                                                        let peers = store
                                                            .ipfs
                                                            .pubsub_peers(Some(topic.clone()))
                                                            .await?;
                                                        let peer_id = did_to_libp2p_pub(&sender)
                                                            .map(|pk| pk.to_peer_id())?;

                                                        match peers.contains(&peer_id) {
                                                            true => {
                                                                if let Err(_e) = store
                                                                    .ipfs
                                                                    .pubsub_publish(
                                                                        topic.clone(),
                                                                        bytes.into(),
                                                                    )
                                                                    .await
                                                                {
                                                                    warn!("Unable to publish to topic. Queuing event");
                                                                    if let Err(e) = store
                                                                        .queue_event(
                                                                            sender.clone(),
                                                                            Queue::direct(
                                                                                conversation_id,
                                                                                None,
                                                                                peer_id,
                                                                                topic.clone(),
                                                                                data.clone(),
                                                                            ),
                                                                        )
                                                                        .await
                                                                    {
                                                                        error!("Error submitting event to queue: {e}");
                                                                    }
                                                                }
                                                            }
                                                            false => {
                                                                if let Err(e) = store
                                                                    .queue_event(
                                                                        sender.clone(),
                                                                        Queue::direct(
                                                                            conversation_id,
                                                                            None,
                                                                            peer_id,
                                                                            topic.clone(),
                                                                            data.clone(),
                                                                        ),
                                                                    )
                                                                    .await
                                                                {
                                                                    error!("Error submitting event to queue: {e}");
                                                                }
                                                            }
                                                        };

                                                        Ok::<_, Error>(())
                                                    }
                                                };
                                                if let Err(e) = result.await {
                                                    error!("Error: {e}");
                                                }
                                            }
                                        }
                                    }
                                    ConversationRequestResponse::Response(response) => {
                                        match response {
                                            crate::store::ConversationResponse::Key {
                                                conversation_id,
                                                key,
                                            } => {
                                                let sender = payload.sender();

                                                let mut keystore = match store
                                                    .conversation_keystore(conversation_id)
                                                    .await
                                                {
                                                    Ok(keystore) => keystore,
                                                    Err(e) => {
                                                        error!("Error obtaining keystore: {e}. Skipping");
                                                        continue;
                                                    }
                                                };

                                                let raw_key = match ecdh_decrypt(
                                                    &did,
                                                    Some(sender.clone()),
                                                    key,
                                                ) {
                                                    Ok(key) => key,
                                                    Err(e) => {
                                                        error!(
                                                            "Error decrypting key: {e}. Skipping"
                                                        );
                                                        continue;
                                                    }
                                                };

                                                if let Err(e) =
                                                    keystore.insert(&did, &sender, raw_key)
                                                {
                                                    match e {
                                                        Error::PublicKeyInvalid => {
                                                            error!("Key already exist in store")
                                                        }
                                                        e => error!(
                                                            "Error inserting key into store: {e}"
                                                        ),
                                                    }
                                                    continue;
                                                }

                                                if let Err(e) = store
                                                    .set_conversation_keystore(
                                                        conversation_id,
                                                        &keystore,
                                                    )
                                                    .await
                                                {
                                                    error!("Error setting keystore: {e}");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        self.stream_reqres_task
            .write()
            .await
            .insert(conversation_id, task);
    }

    async fn request_key(&self, conversation_id: Uuid, did: &DID) -> Result<(), Error> {
        let request =
            ConversationRequestResponse::Request(ConversationRequest::Key { conversation_id });

        let mut conversation = self.get_conversation(conversation_id).await?;

        if !conversation.recipients().contains(did) {
            //TODO: user is not a recipient of the conversation
            return Err(Error::PublicKeyInvalid);
        }

        conversation.messages.clear();
        conversation.recipients.clear();

        let own_did = &self.did;

        let bytes = ecdh_encrypt(own_did, Some(did.clone()), serde_json::to_vec(&request)?)?;
        let signature = sign_serde(own_did, &bytes)?;

        let payload = Payload::new(own_did, &bytes, &signature);

        let bytes = payload.to_bytes()?;

        let topic = conversation.reqres_topic(did);

        let peers = self.ipfs.pubsub_peers(Some(topic.clone())).await?;
        let peer_id = did_to_libp2p_pub(did).map(|pk| pk.to_peer_id())?;

        match peers.contains(&peer_id) {
            true => {
                let bytes = bytes.into();
                if let Err(_e) = self.ipfs.pubsub_publish(topic.clone(), bytes).await {
                    warn!("Unable to publish to topic. Queuing event");
                    if let Err(e) = self
                        .queue_event(
                            did.clone(),
                            Queue::direct(
                                conversation_id,
                                None,
                                peer_id,
                                topic.clone(),
                                payload.data().to_vec(),
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
                        did.clone(),
                        Queue::direct(
                            conversation_id,
                            None,
                            peer_id,
                            topic.clone(),
                            payload.data().to_vec(),
                        ),
                    )
                    .await
                {
                    error!("Error submitting event to queue: {e}");
                }
            }
        };

        Ok(())
    }

    async fn start_task(&self, conversation_id: Uuid, stream: SubscriptionStream) {
        let (tx, mut rx) = unbounded();
        self.conversation_sender
            .write()
            .await
            .insert(conversation_id, tx);

        let (tx, _) = broadcast::channel(1024);

        self.stream_sender.write().await.insert(conversation_id, tx);

        self.conversation_lock
            .write()
            .await
            .insert(conversation_id, Arc::new(Semaphore::new(PERMIT_AMOUNT)));

        info!("Task started for {conversation_id}");
        let did = self.did.clone();

        let task = tokio::spawn({
            let mut store = self.clone();
            async move {
                futures::pin_mut!(stream);
                loop {
                    let (direction, event, ret) = tokio::select! {
                        event = stream.next() => {
                            let Some(event) = event else {
                                continue;
                            };

                            let Ok(data) = Payload::from_bytes(&event.data) else {
                                continue;
                            };

                            let own_did = &*did;

                            let Ok(mut conversation) = store.get_conversation(conversation_id).await else {
                                    continue;
                            };
                            conversation.messages.clear();

                            let bytes_results = match conversation.conversation_type {
                                ConversationType::Direct => {
                                    let Some(recipient) = conversation
                                        .recipients()
                                        .iter()
                                        .filter(|did| own_did.ne(did))
                                        .cloned()
                                        .collect::<Vec<_>>()
                                        .first()
                                        .cloned() else {
                                            continue;
                                        };
                                    ecdh_decrypt(own_did, Some(recipient), data.data())
                                }
                                ConversationType::Group => {

                                    let Ok(key) = store.conversation_keystore(conversation.id()).await.and_then(|keystore| keystore.get_latest(own_did, &data.sender())) else {
                                        continue;
                                    };
                                    Cipher::direct_decrypt(data.data(), &key)
                                }
                            };
                            drop(conversation);

                            let Ok(bytes) = bytes_results else {
                                continue;
                            };

                            let Ok(event) = serde_json::from_slice::<MessagingEvents>(&bytes) else {
                                continue;
                            };

                            (MessageDirection::In, event, None)
                        },
                        event = rx.next() => {
                            let Some((event, ret)) = event else {
                                continue;
                            };

                            (MessageDirection::Out, event, ret)
                        }
                    };

                    let conversation = match store.get_conversation(conversation_id).await {
                        Ok(c) => c,
                        Err(e) => {
                            if let Some(ret) = ret {
                                let _ = ret.send(Err(e)).ok();
                            }
                            continue;
                        }
                    };

                    if let Err(e) = store
                        .message_event(conversation, &event, direction, Default::default())
                        .await
                    {
                        error!("Error processing message: {e}");
                        if let Some(ret) = ret {
                            let _ = ret.send(Err(e)).ok();
                        }
                        continue;
                    }
                    if let Some(ret) = ret {
                        let _ = ret.send(Ok(())).ok();
                    }
                }
            }
        });
        self.stream_task.write().await.insert(conversation_id, task);
        self.start_event_task(conversation_id).await;
        self.start_reqres_task(conversation_id).await;
    }

    async fn end_task(&self, conversation_id: Uuid) {
        if let Some(task) = self
            .stream_reqres_task
            .write()
            .await
            .remove(&conversation_id)
        {
            task.abort();
        }

        if let Some(task) = self
            .stream_event_task
            .write()
            .await
            .remove(&conversation_id)
        {
            task.abort();
        }

        self.stream_sender.write().await.remove(&conversation_id);

        if let Some(task) = self.stream_task.write().await.remove(&conversation_id) {
            info!("Attempting to end task for {conversation_id}");
            task.abort();
            info!("Task for {conversation_id} has ended");
            if let Some(tx) = self
                .conversation_sender
                .write()
                .await
                .remove(&conversation_id)
            {
                tx.close_channel();
            }
            if let Some(permit) = self
                .conversation_lock
                .write()
                .await
                .remove(&conversation_id)
            {
                permit.close();
                drop(permit);
            }
        }
    }

    async fn process_conversation(
        &mut self,
        data: Payload<'_>,
        event: ConversationEvents,
    ) -> anyhow::Result<()> {
        match event {
            ConversationEvents::NewConversation(peer) => {
                let did = &*self.did;
                info!("New conversation event received from {peer}");
                let id = super::generate_shared_topic(did, &peer, Some("direct-conversation"))?;

                if self.exist(id).await {
                    warn!("Conversation with {id} exist");
                    return Ok(());
                }

                if let Ok(true) = self.account.is_blocked(&peer).await {
                    warn!("{peer} is blocked");
                    return Ok(());
                }

                let list = [did.clone(), peer];
                info!("Creating conversation");
                let convo = ConversationDocument::new_direct(did, list)?;
                info!(
                    "{} conversation created: {}",
                    convo.conversation_type,
                    convo.id()
                );

                let cid = convo.to_cid(&self.ipfs).await?;
                if !self.ipfs.is_pinned(&cid).await? {
                    self.ipfs.insert_pin(&cid, false).await?;
                }

                self.conversation_cid.write().await.insert(convo.id(), cid);

                let stream = match self.ipfs.pubsub_subscribe(convo.topic()).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Error subscribing to conversation: {e}");
                        return Ok(());
                    }
                };

                self.start_task(convo.id(), stream).await;

                if let Some(path) = self.path.as_ref() {
                    let cid = cid.to_string();
                    if let Err(e) = tokio::fs::write(path.join(convo.id().to_string()), cid).await {
                        error!("Unable to save info to file: {e}");
                    }
                }
                if let Err(e) = self.event.send(RayGunEventKind::ConversationCreated {
                    conversation_id: convo.id(),
                }) {
                    error!("Error broadcasting event: {e}");
                }
            }
            ConversationEvents::NewGroupConversation(
                creator,
                name,
                conversation_id,
                initial_recipients,
                signature,
            ) => {
                let did = &*self.did;
                info!("New group conversation event received");

                if self.exist(conversation_id).await {
                    warn!("Conversation with {conversation_id} exist");
                    return Ok(());
                }

                info!("Creating conversation");
                let convo = ConversationDocument::new(
                    did,
                    name,
                    initial_recipients.clone(),
                    Some(conversation_id),
                    ConversationType::Group,
                    Some(creator),
                    signature,
                )?;
                info!(
                    "{} conversation created: {}",
                    convo.conversation_type,
                    convo.id()
                );

                let mut keystore = Keystore::new(conversation_id);
                keystore.insert(did, did, warp::crypto::generate(64))?;

                //Although we verify internally, this is just as a precaution
                convo.verify()?;

                let cid = convo.to_cid(&self.ipfs).await?;
                if !self.ipfs.is_pinned(&cid).await? {
                    self.ipfs.insert_pin(&cid, false).await?;
                }
                self.conversation_cid.write().await.insert(convo.id(), cid);

                let stream = match self.ipfs.pubsub_subscribe(convo.topic()).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        error!("Error subscribing to conversation: {e}");
                        return Ok(());
                    }
                };

                self.set_conversation_keystore(conversation_id, &keystore)
                    .await?;

                self.start_task(convo.id(), stream).await;
                if let Some(path) = self.path.as_ref() {
                    let cid = cid.to_string();
                    if let Err(e) = tokio::fs::write(path.join(convo.id().to_string()), cid).await {
                        error!("Unable to save info to file: {e}");
                    }
                }
                if let Err(e) = self.event.send(RayGunEventKind::ConversationCreated {
                    conversation_id: convo.id(),
                }) {
                    error!("Error broadcasting event: {e}");
                }

                tokio::spawn({
                    let store = self.clone();
                    let recipients = initial_recipients
                        .iter()
                        .filter(|d| did.ne(d))
                        .cloned()
                        .collect::<Vec<_>>();
                    async move {
                        for recipient in recipients {
                            if let Err(_e) = store.request_key(conversation_id, &recipient).await {}
                        }
                    }
                });
            }
            ConversationEvents::DeleteConversation(conversation_id) => {
                trace!("Delete conversation event received for {conversation_id}");
                if !self.exist(conversation_id).await {
                    anyhow::bail!("Conversation {conversation_id} doesnt exist");
                }

                let sender = data.sender();

                match self.get_conversation(conversation_id).await {
                    Ok(conversation)
                        if conversation.recipients().contains(&sender)
                            && matches!(
                                conversation.conversation_type,
                                ConversationType::Direct
                            )
                            || matches!(
                                conversation.conversation_type,
                                ConversationType::Group
                            ) && matches!(conversation.creator.clone(), Some(creator) if creator.eq(&sender)) =>
                    {
                        conversation
                    }
                    _ => {
                        anyhow::bail!("Conversation exist but did not match condition required");
                    }
                };

                self.end_task(conversation_id).await;

                let conversation_cid = self
                    .conversation_cid
                    .write()
                    .await
                    .remove(&conversation_id)
                    .ok_or(Error::InvalidConversation)?;

                if self.ipfs.is_pinned(&conversation_cid).await? {
                    if let Err(e) = self.ipfs.remove_pin(&conversation_cid, false).await {
                        error!("Unable to remove pin from {conversation_cid}: {e}");
                    }
                }

                let mut document: ConversationDocument =
                    conversation_cid.get_local_dag(&self.ipfs).await?;
                let topic = document.topic();
                self.queue.write().await.remove(&sender);

                tokio::spawn({
                    let ipfs = self.ipfs.clone();
                    async move {
                        let _ = document.delete_all_message(ipfs.clone()).await.ok();
                        ipfs.remove_block(conversation_cid).await.ok();
                    }
                });

                if self.ipfs.pubsub_unsubscribe(&topic).await.is_ok() {
                    warn!("topic should have been unsubscribed after dropping conversation.");
                }

                if let Some(path) = self.path.as_ref() {
                    if let Err(e) =
                        tokio::fs::remove_file(path.join(conversation_id.to_string())).await
                    {
                        error!("Unable to remove conversation: {e}");
                    }
                }

                if let Err(e) = self
                    .event
                    .send(RayGunEventKind::ConversationDeleted { conversation_id })
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
                                if let Err(_e) = ecdh_decrypt(&self.did, Some(did.clone()), &data) {
                                    //This validates that the data itself was encrypted by the sender
                                    continue;
                                }

                                let signature = match sign_serde(&self.did, &data) {
                                    Ok(sig) => sig,
                                    Err(_e) => {
                                        continue;
                                    }
                                };

                                let payload = Payload::new(&self.did, data, &signature);

                                let bytes = match payload.to_bytes() {
                                    Ok(bytes) => bytes.into(),
                                    Err(_e) => {
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

impl MessageStore {
    pub async fn create_conversation(&mut self, did_key: &DID) -> Result<Conversation, Error> {
        if self.with_friends.load(Ordering::SeqCst) {
            self.account.has_friend(did_key).await?;
        }

        if let Ok(true) = self.account.is_blocked(did_key).await {
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
                if let Ok(list) = account.get_identity(did.into()).await {
                    if list.is_empty() {
                        warn!("Unable to find identity. Creating conversation anyway");
                    }
                }
            }
        });

        let conversation =
            ConversationDocument::new_direct(own_did, [own_did.clone(), did_key.clone()])?;

        let cid = conversation.to_cid(&self.ipfs).await?;

        let convo_id = conversation.id();
        let topic = conversation.topic();

        self.conversation_cid.write().await.insert(convo_id, cid);

        let stream = self.ipfs.pubsub_subscribe(topic).await?;

        self.start_task(conversation.id(), stream).await;

        let peer_id = did_to_libp2p_pub(did_key)?.to_peer_id();

        let event = ConversationEvents::NewConversation(own_did.clone());

        let bytes = ecdh_encrypt(own_did, Some(did_key.clone()), serde_json::to_vec(&event)?)?;
        let signature = sign_serde(own_did, &bytes)?;

        let payload = Payload::new(own_did, &bytes, &signature);

        let topic = format!("{did_key}/messaging");
        let peers = self.ipfs.pubsub_peers(Some(topic.clone())).await?;

        match peers.contains(&peer_id) {
            true => {
                if let Err(_e) = self
                    .ipfs
                    .pubsub_publish(topic.clone(), payload.to_bytes()?.into())
                    .await
                {
                    warn!("Unable to publish to topic. Queuing event");
                    if let Err(e) = self
                        .queue_event(
                            did_key.clone(),
                            Queue::direct(
                                convo_id,
                                None,
                                peer_id,
                                topic.clone(),
                                payload.data().to_vec(),
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
                        did_key.clone(),
                        Queue::direct(
                            convo_id,
                            None,
                            peer_id,
                            topic.clone(),
                            payload.data().to_vec(),
                        ),
                    )
                    .await
                {
                    error!("Error submitting event to queue: {e}");
                }
            }
        };

        if !self.disable_sender_event_emit.load(Ordering::Relaxed) {
            if let Err(e) = self.event.send(RayGunEventKind::ConversationCreated {
                conversation_id: conversation.id(),
            }) {
                error!("Error broadcasting event: {e}");
            }
        }
        Ok(Conversation::from(&conversation))
    }

    pub async fn create_group_conversation(
        &mut self,
        name: Option<String>,
        did_key: HashSet<DID>,
    ) -> Result<Conversation, Error> {
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

        if self.with_friends.load(Ordering::SeqCst) {
            for did in did_key.iter() {
                self.account.has_friend(did).await?;
            }
        }

        for did in did_key.iter() {
            if let Ok(true) = self.account.is_blocked(did).await {
                return Err(Error::PublicKeyIsBlocked);
            }
        }

        let own_did = &*(self.did.clone());

        if did_key.contains(own_did) {
            return Err(Error::CannotCreateConversation);
        }

        //Temporary limit
        if self.list_conversations().await.unwrap_or_default().len() >= 32 {
            return Err(Error::ConversationLimitReached);
        }

        tokio::spawn({
            let account = self.account.clone();
            let did_list = Vec::from_iter(did_key.clone());
            async move {
                if let Ok(list) = account
                    .get_identity(warp::multipass::identity::Identifier::DIDList(did_list))
                    .await
                {
                    if list.is_empty() {
                        warn!("Unable to find identities. Creating conversation anyway");
                    }
                }
            }
        });

        let conversation =
            ConversationDocument::new_group(own_did, name, &Vec::from_iter(did_key))?;

        let recipient = conversation.recipients();

        let cid = conversation.to_cid(&self.ipfs).await?;

        let convo_id = conversation.id();
        let topic = conversation.topic();

        self.conversation_cid.write().await.insert(convo_id, cid);
        let mut keystore = Keystore::new(conversation.id());
        keystore.insert(own_did, own_did, warp::crypto::generate(64))?;
        self.set_conversation_keystore(convo_id, &keystore).await?;

        let stream = self.ipfs.pubsub_subscribe(topic).await?;

        self.start_task(conversation.id(), stream).await;

        let peer_id_list = recipient
            .clone()
            .iter()
            .filter(|did| own_did.ne(did))
            .map(|did| (did.clone(), did))
            .filter_map(|(a, b)| did_to_libp2p_pub(b).map(|pk| (a, pk)).ok())
            .map(|(did, pk)| (did, pk.to_peer_id()))
            .collect::<Vec<_>>();

        if let Some(path) = self.path.as_ref() {
            let cid = cid.to_string();

            if let Err(e) = tokio::fs::write(path.join(conversation.id().to_string()), cid).await {
                error!("Unable to save info to file: {e}");
            }
        }
        let event = serde_json::to_vec(&ConversationEvents::NewGroupConversation(
            own_did.clone(),
            conversation.name(),
            conversation.id(),
            recipient.clone(),
            conversation.signature.clone(),
        ))?;

        for (did, peer_id) in peer_id_list {
            let bytes = ecdh_encrypt(own_did, Some(did.clone()), &event)?;
            let signature = sign_serde(own_did, &bytes)?;

            let payload = Payload::new(own_did, &bytes, &signature);

            let topic = format!("{did}/messaging");
            let peers = self.ipfs.pubsub_peers(Some(topic.clone())).await?;
            match peers.contains(&peer_id) {
                true => {
                    if let Err(_e) = self
                        .ipfs
                        .pubsub_publish(topic.clone(), payload.to_bytes()?.into())
                        .await
                    {
                        warn!("Unable to publish to topic. Queuing event");
                        if let Err(e) = self
                            .queue_event(
                                did,
                                Queue::direct(
                                    convo_id,
                                    None,
                                    peer_id,
                                    topic.clone(),
                                    payload.data().to_vec(),
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
                            did,
                            Queue::direct(
                                convo_id,
                                None,
                                peer_id,
                                topic.clone(),
                                payload.data().to_vec(),
                            ),
                        )
                        .await
                    {
                        error!("Error submitting event to queue: {e}");
                    }
                }
            };
        }

        if !self.disable_sender_event_emit.load(Ordering::Relaxed) {
            if let Err(e) = self.event.send(RayGunEventKind::ConversationCreated {
                conversation_id: conversation.id(),
            }) {
                error!("Error broadcasting event: {e}");
            }
        }

        tokio::spawn({
            let store = self.clone();
            let conversation_id = conversation.id();
            let recipients = recipient
                .iter()
                .filter(|d| own_did.ne(d))
                .cloned()
                .collect::<Vec<_>>();
            async move {
                for recipient in recipients {
                    if let Err(_e) = store.request_key(conversation_id, &recipient).await {}
                }
            }
        });

        Ok(Conversation::from(&conversation))
    }

    pub async fn delete_conversation(
        &mut self,
        conversation_id: Uuid,
        broadcast: bool,
    ) -> Result<(), Error> {
        self.end_task(conversation_id).await;

        let conversation_cid = self
            .conversation_cid
            .write()
            .await
            .remove(&conversation_id)
            .ok_or(Error::InvalidConversation)?;

        if self.ipfs.is_pinned(&conversation_cid).await? {
            if let Err(e) = self.ipfs.remove_pin(&conversation_cid, false).await {
                error!("Unable to remove pin from {conversation_cid}: {e}");
            }
        }
        let mut document_type: ConversationDocument =
            conversation_cid.get_local_dag(&self.ipfs).await?;

        self.ipfs.remove_block(conversation_cid).await?;

        if broadcast {
            let recipients = document_type.recipients();

            let own_did = &*self.did;

            let peer_id_list = recipients
                .clone()
                .iter()
                .filter(|did| own_did.ne(did))
                .map(|did| (did.clone(), did))
                .filter_map(|(a, b)| did_to_libp2p_pub(b).map(|pk| (a, pk)).ok())
                .map(|(did, pk)| (did, pk.to_peer_id()))
                .collect::<Vec<_>>();

            let event =
                serde_json::to_vec(&ConversationEvents::DeleteConversation(document_type.id()))?;

            for (recipient, peer_id) in peer_id_list {
                let bytes = ecdh_encrypt(own_did, Some(recipient.clone()), &event)?;
                let signature = sign_serde(own_did, &bytes)?;

                let payload = Payload::new(own_did, &bytes, &signature);
                let topic = format!("{recipient}/messaging");

                let peers = self.ipfs.pubsub_peers(Some(topic.clone())).await?;

                match peers.contains(&peer_id) {
                    true => {
                        if let Err(e) = self
                            .ipfs
                            .pubsub_publish(topic.clone(), payload.to_bytes()?.into())
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
                                        topic.clone(),
                                        payload.data().to_vec(),
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
                                    topic.clone(),
                                    payload.data().to_vec(),
                                ),
                            )
                            .await
                        {
                            error!("Error submitting event to queue: {e}");
                        }
                    }
                };
            }
        }

        let conversation_id = document_type.id();
        tokio::spawn({
            let ipfs = self.ipfs.clone();
            async move {
                let _ = document_type.delete_all_message(ipfs).await.is_ok();
            }
        });

        if let Some(path) = self.path.as_ref() {
            if let Err(e) = tokio::fs::remove_file(path.join(conversation_id.to_string())).await {
                error!("Unable to remove conversation: {e}");
            }
        }

        if let Err(e) = self
            .event
            .send(RayGunEventKind::ConversationDeleted { conversation_id })
        {
            error!("Error broadcasting event: {e}");
        }
        Ok(())
    }

    async fn conversation_queue(
        &self,
        conversation_id: Uuid,
    ) -> Result<OwnedSemaphorePermit, Error> {
        let permit = self
            .conversation_lock
            .read()
            .await
            .get(&conversation_id)
            .cloned()
            .ok_or(Error::InvalidConversation)?;

        permit
            .acquire_owned()
            .await
            .map_err(anyhow::Error::from)
            .map_err(Error::from)
    }

    pub async fn load_conversations(&self, background: bool) -> Result<(), Error> {
        let Some(path) = self.path.as_ref() else {
            return Ok(())
        };

        if !path.is_dir() {
            return Err(Error::InvalidDirectory);
        }

        let mut entry_stream = ReadDirStream::new(tokio::fs::read_dir(path).await?);

        while let Some(entry) = entry_stream.next().await {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_file() && !entry_path.ends_with(".messaging_queue") {
                let Some(filename) = entry_path.file_name().map(|file| file.to_string_lossy().to_string()) else {
                    continue
                };

                //TODO: Maybe check file extension instead
                let slices = filename.split('.').collect::<Vec<&str>>();

                let keystore = slices
                    .last()
                    .map(|s| s.ends_with("keystore"))
                    .unwrap_or_default();

                let Some(file_id) = slices.first() else {
                    continue
                };

                let Ok(id) = Uuid::from_str(file_id) else {
                    continue
                };

                let Ok(cid_str) = tokio::fs::read(entry_path).await.map(|bytes| String::from_utf8_lossy(&bytes).to_string()) else {
                    continue
                };
                if let Ok(cid) = cid_str.parse::<Cid>() {
                    if keystore {
                        self.conversation_keystore_cid.write().await.insert(id, cid);
                    } else {
                        let task = {
                            let store = self.clone();
                            async move {
                                let conversation: ConversationDocument =
                                    cid.get_local_dag(&store.ipfs).await?;
                                conversation.verify()?;
                                store.conversation_cid.write().await.insert(id, cid);

                                let stream =
                                    store.ipfs.pubsub_subscribe(conversation.topic()).await?;

                                store.start_task(conversation.id(), stream).await;

                                Ok::<_, Error>(())
                            }
                        };

                        if background {
                            tokio::spawn(task);
                        } else if let Err(e) = task.await {
                            error!("Error loading conversation: {e}");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn list_conversation_documents(&self) -> Result<Vec<ConversationDocument>, Error> {
        let list = FuturesUnordered::from_iter(
            self.conversation_cid
                .read()
                .await
                .values()
                .map(|cid| async { (*cid).get_local_dag(&self.ipfs).await }),
        )
        .filter_map(|res| async { res.ok() })
        .collect::<Vec<_>>()
        .await;
        Ok(list)
    }

    pub async fn list_conversations(&self) -> Result<Vec<Conversation>, Error> {
        self.list_conversation_documents()
            .await
            .map(|list| list.iter().map(|document| document.into()).collect())
    }

    pub async fn messages_count(&self, conversation_id: Uuid) -> Result<usize, Error> {
        let conversation = self.get_conversation(conversation_id).await?;
        Ok(conversation.messages.len())
    }

    pub async fn conversation_keystore(&self, conversation_id: Uuid) -> Result<Keystore, Error> {
        let guard = self.conversation_keystore_cid.read().await;

        let cid = guard
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;

        cid.get_local_dag(&self.ipfs).await
    }

    pub async fn set_conversation_keystore(
        &self,
        conversation_id: Uuid,
        keystore: &Keystore,
    ) -> Result<(), Error> {
        let cid = keystore.to_cid(&self.ipfs).await?;

        if !self.ipfs.is_pinned(&cid).await? {
            self.ipfs.insert_pin(&cid, false).await?;
        }

        let old_cid = self
            .conversation_keystore_cid
            .write()
            .await
            .insert(conversation_id, cid);

        if let Some(old_cid) = old_cid {
            if old_cid != cid {
                if self.ipfs.is_pinned(&old_cid).await? {
                    self.ipfs.insert_pin(&old_cid, false).await?;
                }
                if let Err(_e) = self.ipfs.remove_block(old_cid).await {}
            }
        }

        if let Some(path) = self.path.as_ref() {
            let keystore_cid = cid.to_string();
            if let Err(e) = tokio::fs::write(
                path.join(format!("{}.keystore", conversation_id)),
                keystore_cid,
            )
            .await
            {
                error!("Unable to save info to file: {e}");
            }
        }

        Ok(())
    }

    async fn send_single_conversation_event(
        &mut self,
        did_key: &DID,
        conversation_id: Uuid,
        event: ConversationEvents,
    ) -> Result<(), Error> {
        let own_did = &*self.did;

        let event = serde_json::to_vec(&event)?;

        let bytes = ecdh_encrypt(own_did, Some(did_key.clone()), &event)?;
        let signature = sign_serde(own_did, &bytes)?;

        let payload = Payload::new(own_did, &bytes, &signature);

        let peer_id = did_to_libp2p_pub(did_key)?.to_peer_id();
        let topic = format!("{did_key}/messaging");
        let peers = self.ipfs.pubsub_peers(Some(topic.clone())).await?;
        match peers.contains(&peer_id) {
            true => {
                let bytes = payload.to_bytes()?.into();
                if let Err(_e) = self.ipfs.pubsub_publish(topic.clone(), bytes).await {
                    warn!("Unable to publish to topic. Queuing event");
                    if let Err(e) = self
                        .queue_event(
                            did_key.clone(),
                            Queue::direct(
                                conversation_id,
                                None,
                                peer_id,
                                topic.clone(),
                                payload.data().to_vec(),
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
                        did_key.clone(),
                        Queue::direct(
                            conversation_id,
                            None,
                            peer_id,
                            topic.clone(),
                            payload.data().to_vec(),
                        ),
                    )
                    .await
                {
                    error!("Error submitting event to queue: {e}");
                }
            }
        };

        Ok(())
    }

    pub async fn get_message(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<Message, Error> {
        let conversation = self.get_conversation(conversation_id).await?;
        let keystore = match conversation.conversation_type {
            ConversationType::Direct => None,
            ConversationType::Group => self.conversation_keystore(conversation.id()).await.ok(),
        };
        conversation
            .get_message(&self.ipfs, self.did.clone(), message_id, keystore.as_ref())
            .await
    }

    pub async fn message_status(
        &self,
        conversation_id: Uuid,
        message_id: Uuid,
    ) -> Result<MessageStatus, Error> {
        let conversation = self.get_conversation(conversation_id).await?;

        if matches!(conversation.conversation_type, ConversationType::Group) {
            //TODO: Handle message status for group
            return Err(Error::Unimplemented);
        }

        if !conversation
            .messages
            .iter()
            .any(|document| document.id == message_id)
        {
            return Err(Error::MessageNotFound);
        }

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
    ) -> Result<Messages, Error> {
        let conversation = self.get_conversation(conversation).await?;
        let keystore = match conversation.conversation_type {
            ConversationType::Direct => None,
            ConversationType::Group => self.conversation_keystore(conversation.id()).await.ok(),
        };

        let m_type = opt.messages_type();
        match m_type {
            MessagesType::Stream => {
                let stream = conversation
                    .get_messages_stream(&self.ipfs, self.did.clone(), opt, keystore.as_ref())
                    .await?;
                Ok(Messages::Stream(MessageStream(stream)))
            }
            MessagesType::List => {
                let list = conversation
                    .get_messages(&self.ipfs, self.did.clone(), opt, keystore.as_ref())
                    .await
                    .map(Vec::from_iter)?;
                Ok(Messages::List(list))
            }
            MessagesType::Pages { .. } => {
                conversation
                    .get_messages_pages(&self.ipfs, self.did.clone(), opt, keystore.as_ref())
                    .await
            }
        }
    }

    pub async fn exist(&self, conversation: Uuid) -> bool {
        self.conversation_cid
            .read()
            .await
            .contains_key(&conversation)
    }

    pub async fn get_conversation(
        &self,
        conversation_id: Uuid,
    ) -> Result<ConversationDocument, Error> {
        let map = self.conversation_cid.read().await;
        let cid = map
            .get(&conversation_id)
            .ok_or(Error::InvalidConversation)?;
        let conversation: ConversationDocument = (*cid).get_local_dag(&self.ipfs).await?;
        conversation.verify().map(|_| conversation)
    }

    pub async fn get_conversation_mut<F: FnOnce(&mut ConversationDocument)>(
        &self,
        conversation_id: Uuid,
        func: F,
    ) -> Result<(), Error> {
        let document = &mut self.get_conversation(conversation_id).await?;

        let own_did = &*self.did;

        func(document);

        if let Some(creator) = document.creator.as_ref() {
            if creator.eq(own_did) {
                document.sign(own_did)?;
            }
        }

        document.verify()?;

        let new_cid = document.to_cid(&self.ipfs).await?;

        let old_cid = self
            .conversation_cid
            .write()
            .await
            .insert(conversation_id, new_cid);

        if let Some(old_cid) = old_cid {
            if new_cid != old_cid {
                if self.ipfs.is_pinned(&old_cid).await? {
                    if let Err(e) = self.ipfs.remove_pin(&old_cid, false).await {
                        error!("Unable to remove pin on {old_cid}: {e}");
                    }
                }
                if let Err(e) = self.ipfs.remove_block(old_cid).await {
                    error!("Unable to remove {old_cid}: {e}");
                }
            }
        }

        if let Some(path) = self.path.as_ref() {
            let cid = new_cid.to_string();
            if let Err(e) = tokio::fs::write(path.join(conversation_id.to_string()), cid).await {
                error!("Unable to save info to file: {e}");
            }
        }
        Ok(())
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
        let rx = self
            .get_conversation_sender(conversation_id)
            .await?
            .subscribe();
        Ok(rx)
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
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(_) => {}
                };
            }
        })
    }

    pub async fn update_conversation_name(
        &mut self,
        conversation_id: Uuid,
        name: &str,
    ) -> Result<(), Error> {
        let name_length = name.trim().len();

        if name_length == 0 || name_length > 255 {
            return Err(Error::InvalidLength {
                context: "name".into(),
                current: name_length,
                minimum: Some(1),
                maximum: Some(255),
            });
        }

        let conversation = self.get_conversation(conversation_id).await?;

        if matches!(conversation.conversation_type, ConversationType::Direct) {
            return Err(Error::InvalidConversation);
        }

        let Some(creator) = conversation.creator.clone() else {
            return Err(Error::InvalidConversation);
        };

        let own_did = &*self.did;

        if creator.ne(own_did) {
            return Err(Error::PublicKeyInvalid);
        }

        self.get_conversation_mut(conversation_id, |conversation| {
            conversation.name = Some(name.to_string());
        })
        .await?;

        let conversation = self.get_conversation(conversation_id).await?;

        let Some(signature) = conversation.signature.clone() else {
            return Err(Error::InvalidSignature);
        };

        let event =
            MessagingEvents::UpdateConversationName(conversation_id, name.to_string(), signature);

        let tx = self.get_conversation_sender(conversation_id).await?;
        let _ = tx.send(MessageEventKind::ConversationNameUpdated {
            conversation_id,
            name: name.to_string(),
        });

        self.send_raw_event(conversation_id, None, event, true)
            .await
    }

    pub async fn add_recipient(
        &mut self,
        conversation_id: Uuid,
        did_key: &DID,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;
        let _guard = self.conversation_queue(conversation_id).await?;

        if matches!(conversation.conversation_type, ConversationType::Direct) {
            return Err(Error::InvalidConversation);
        }

        let Some(creator) = conversation.creator.clone() else {
            return Err(Error::InvalidConversation);
        };

        let own_did = &*self.did;

        if creator.ne(own_did) {
            return Err(Error::PublicKeyInvalid);
        }

        if creator.eq(did_key) {
            return Err(Error::PublicKeyInvalid);
        }

        if self.account.is_blocked(did_key).await? {
            return Err(Error::PublicKeyIsBlocked);
        }

        if conversation.recipients.contains(did_key) {
            return Err(Error::IdentityExist);
        }

        self.get_conversation_mut(conversation_id, |conversation| {
            conversation.recipients.push(did_key.clone());
        })
        .await?;
        drop(_guard);

        let conversation = self.get_conversation(conversation_id).await?;

        let Some(signature) = conversation.signature.clone() else {
            return Err(Error::InvalidSignature);
        };

        let event = MessagingEvents::AddRecipient(
            conversation_id,
            did_key.clone(),
            conversation.recipients(),
            signature.clone(),
        );

        let tx = self.get_conversation_sender(conversation_id).await?;
        let _ = tx.send(MessageEventKind::RecipientAdded {
            conversation_id,
            recipient: did_key.clone(),
        });

        self.send_raw_event(conversation_id, None, event, true)
            .await?;

        let own_did = &*self.did;
        let new_event = ConversationEvents::NewGroupConversation(
            own_did.clone(),
            conversation.name(),
            conversation.id(),
            conversation.recipients(),
            Some(signature),
        );

        self.send_single_conversation_event(did_key, conversation_id, new_event)
            .await?;

        if let Err(_e) = self.request_key(conversation_id, did_key).await {}
        Ok(())
    }

    pub async fn remove_recipient(
        &mut self,
        conversation_id: Uuid,
        did_key: &DID,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;
        let _guard = self.conversation_queue(conversation_id).await?;

        if matches!(conversation.conversation_type, ConversationType::Direct) {
            return Err(Error::InvalidConversation);
        }

        let Some(creator) = conversation.creator.clone() else {
            return Err(Error::InvalidConversation);
        };

        let own_did = &*self.did;

        if creator.ne(own_did) {
            return Err(Error::PublicKeyInvalid);
        }

        if creator.eq(did_key) {
            return Err(Error::PublicKeyInvalid);
        }

        if !conversation.recipients.contains(did_key) {
            return Err(Error::IdentityDoesntExist);
        }

        self.get_conversation_mut(conversation_id, |conversation| {
            // conversation.recipients.push(did_key.clone());
            conversation.recipients.retain(|did| did.ne(did_key));
        })
        .await?;
        drop(_guard);

        let conversation = self.get_conversation(conversation_id).await?;

        let Some(signature) = conversation.signature.clone() else {
            return Err(Error::InvalidSignature);
        };

        let event = MessagingEvents::RemoveRecipient(
            conversation_id,
            did_key.clone(),
            conversation.recipients(),
            signature.clone(),
        );

        let tx = self.get_conversation_sender(conversation_id).await?;
        let _ = tx.send(MessageEventKind::RecipientRemoved {
            conversation_id,
            recipient: did_key.clone(),
        });

        self.send_raw_event(conversation_id, None, event, true)
            .await?;

        let new_event = ConversationEvents::DeleteConversation(conversation.id());

        self.send_single_conversation_event(did_key, conversation.id(), new_event)
            .await
    }

    pub async fn conversation_tx(
        &self,
        conversation_id: Uuid,
    ) -> Result<ConversationSender, Error> {
        self.conversation_sender
            .read()
            .await
            .get(&conversation_id)
            .cloned()
            .ok_or(Error::InvalidConversation)
    }

    pub async fn send_message(
        &mut self,
        conversation_id: Uuid,
        messages: Vec<String>,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;
        let mut tx = self.conversation_tx(conversation_id).await?;

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
            error!("Length of message is invalid: Got {lines_value_length}; Expected 4096");
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

        tx.send((event.clone(), None))
            .await
            .map_err(anyhow::Error::from)?;

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
        let mut tx = self.conversation_tx(conversation_id).await?;
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
            error!("Length of message is invalid: Got {lines_value_length}; Expected 4096");
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

        tx.send((event.clone(), None))
            .await
            .map_err(anyhow::Error::from)?;

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
        let mut tx = self.conversation_tx(conversation_id).await?;

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
            error!("Length of message is invalid: Got {lines_value_length}; Expected 4096");
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

        tx.send((event.clone(), None))
            .await
            .map_err(anyhow::Error::from)?;

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
        let mut tx = self.conversation_tx(conversation_id).await?;

        let event = MessagingEvents::Delete(conversation.id(), message_id);

        tx.send((event.clone(), None))
            .await
            .map_err(anyhow::Error::from)?;

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
        let mut tx = self.conversation_tx(conversation_id).await?;

        let own_did = &*self.did;

        let event = MessagingEvents::Pin(conversation.id(), own_did.clone(), message_id, state);

        tx.send((event.clone(), None))
            .await
            .map_err(anyhow::Error::from)?;

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
        let mut tx = self.conversation_tx(conversation_id).await?;

        let own_did = &*self.did;

        let event =
            MessagingEvents::React(conversation.id(), own_did.clone(), message_id, state, emoji);

        tx.send((event.clone(), None))
            .await
            .map_err(anyhow::Error::from)?;

        self.send_raw_event(conversation_id, None, event, true)
            .await
    }

    #[allow(clippy::await_holding_lock)]
    //TODO: Return a vector of streams for events of progression for uploading (possibly passing it through to raygun events)
    pub async fn attach(
        &mut self,
        conversation_id: Uuid,
        message_id: Option<Uuid>,
        location: Location,
        files: Vec<PathBuf>,
        messages: Vec<String>,
    ) -> Result<(), Error> {
        if !messages.is_empty() {
            let lines_value_length: usize = messages
                .iter()
                .filter(|s| !s.is_empty())
                .map(|s| s.trim())
                .map(|s| s.chars().count())
                .sum();

            if lines_value_length > 4096 {
                error!("Length of message is invalid: Got {lines_value_length}; Expected 4096");
                return Err(Error::InvalidLength {
                    context: "message".into(),
                    current: lines_value_length,
                    minimum: None,
                    maximum: Some(4096),
                });
            }
        }
        let conversation = self.get_conversation(conversation_id).await?;
        let mut tx = self.conversation_tx(conversation_id).await?;
        //TODO: Send directly if constellation isnt present
        //      this will require uploading to ipfs directly from here
        //      or setting up a separate stream channel related to
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
        message.set_replied(message_id);

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

        tx.send((event.clone(), None))
            .await
            .map_err(anyhow::Error::from)?;

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
                        match ipfs.identity(Some(peer_id)).await {
                            Ok(info) => {
                                //This is done to insure we can successfully exchange blocks
                                let opt = DialOpts::peer_id(peer_id)
                                    .addresses(info.listen_addrs)
                                    .extend_addresses_through_behaviour()
                                    .build();

                                if let Err(e) = ipfs.connect(opt).await {
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
        self.send_message_event(conversation_id, event).await
    }

    pub async fn cancel_event(
        &mut self,
        conversation_id: Uuid,
        event: MessageEvent,
    ) -> Result<(), Error> {
        let conversation = self.get_conversation(conversation_id).await?;
        let own_did = &*self.did;

        let event = MessagingEvents::Event(conversation.id(), own_did.clone(), event, true);
        self.send_message_event(conversation_id, event).await
    }

    pub async fn send_message_event(
        &mut self,
        conversation_id: Uuid,
        event: MessagingEvents,
    ) -> Result<(), Error> {
        let mut conversation = self.get_conversation(conversation_id).await?;
        conversation.messages.clear();

        let own_did = &*self.did;

        let event = serde_json::to_vec(&event)?;

        //TODO: Send with Payload instead
        let bytes = match conversation.conversation_type {
            ConversationType::Direct => {
                let recipient = conversation
                    .recipients()
                    .iter()
                    .filter(|did| own_did.ne(did))
                    .cloned()
                    .collect::<Vec<_>>()
                    .first()
                    .cloned()
                    .ok_or(Error::InvalidConversation)?;
                ecdh_encrypt(own_did, Some(recipient), &event)?
            }
            ConversationType::Group => {
                let keystore = self.conversation_keystore(conversation.id()).await?;
                let key = keystore.get_latest(own_did, own_did)?;
                Cipher::direct_encrypt(&event, &key)?
            }
        };

        let signature = sign_serde(own_did, &bytes)?;
        let payload = Payload::new(own_did, &bytes, &signature);

        let peers = self
            .ipfs
            .pubsub_peers(Some(conversation.event_topic()))
            .await?;

        if !peers.is_empty() {
            if let Err(e) = self
                .ipfs
                .pubsub_publish(conversation.event_topic(), payload.to_bytes()?.into())
                .await
            {
                error!("Unable to send event: {e}");
            }
        }
        Ok(())
    }

    pub async fn send_raw_event<S: Serialize + Send + Sync>(
        &mut self,
        conversation: Uuid,
        message_id: Option<Uuid>,
        event: S,
        queue: bool,
    ) -> Result<(), Error> {
        let mut conversation = self.get_conversation(conversation).await?;
        conversation.messages.clear();

        let own_did = &*self.did;

        let event = serde_json::to_vec(&event)?;

        let bytes = match conversation.conversation_type {
            ConversationType::Direct => {
                let recipient = conversation
                    .recipients()
                    .iter()
                    .filter(|did| own_did.ne(did))
                    .cloned()
                    .collect::<Vec<_>>()
                    .first()
                    .cloned()
                    .ok_or(Error::InvalidConversation)?;
                ecdh_encrypt(own_did, Some(recipient), &event)?
            }
            ConversationType::Group => {
                let keystore = self.conversation_keystore(conversation.id()).await?;
                let key = keystore.get_latest(own_did, own_did)?;
                Cipher::direct_encrypt(&event, &key)?
            }
        };

        let signature = sign_serde(own_did, &bytes)?;

        let payload = Payload::new(own_did, &bytes, &signature);

        let peers = self.ipfs.pubsub_peers(Some(conversation.topic())).await?;

        let mut can_publish = false;

        for recipient in conversation
            .recipients()
            .iter()
            .filter(|did| own_did.ne(did))
        {
            let peer_id = did_to_libp2p_pub(recipient)?.to_peer_id();

            match peers.contains(&peer_id) {
                true => {
                    can_publish = true;
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
                                    payload.data().to_vec(),
                                ),
                            )
                            .await
                        {
                            error!("Error submitting event to queue: {e}");
                        }
                    }
                }
            };
        }

        if can_publish {
            if let Err(_e) = self
                .ipfs
                .pubsub_publish(conversation.topic(), payload.to_bytes()?.into())
                .await
            {}
        }

        Ok(())
    }

    async fn queue_event(&self, did: DID, queue: Queue) -> Result<(), Error> {
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

    async fn message_event(
        &mut self,
        document: ConversationDocument,
        events: &MessagingEvents,
        direction: MessageDirection,
        opt: EventOpt,
    ) -> Result<bool, Error> {
        let _guard = self.conversation_queue(document.id()).await?;
        let tx = self.get_conversation_sender(document.id()).await?;
        let keystore = match document.conversation_type {
            ConversationType::Direct => None,
            ConversationType::Group => self.conversation_keystore(document.id()).await.ok(),
        };

        match events.clone() {
            MessagingEvents::New(mut message) => {
                if document
                    .messages
                    .iter()
                    .any(|message_document| message_document.id == message.id())
                {
                    return Err(Error::MessageFound);
                }

                if !document.recipients().contains(&message.sender()) {
                    return Err(Error::IdentityDoesntExist);
                }

                let lines_value_length: usize = message
                    .value()
                    .iter()
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.chars().count())
                    .sum();

                if lines_value_length == 0 && lines_value_length > 4096 {
                    error!("Length of message is invalid: Got {lines_value_length}; Expected 4096");
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

                let message_document =
                    MessageDocument::new(&self.ipfs, self.did.clone(), message, keystore.as_ref())
                        .await?;

                self.get_conversation_mut(document.id(), |conversation_document| {
                    conversation_document.messages.insert(message_document);
                })
                .await?;

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

                if let Err(e) = tx.send(event) {
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
                    .resolve(&self.ipfs, self.did.clone(), keystore.as_ref())
                    .await?;

                let lines_value_length: usize = val
                    .iter()
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.chars().count())
                    .sum();

                if lines_value_length == 0 && lines_value_length > 4096 {
                    error!("Length of message is invalid: Got {lines_value_length}; Expected 4096");
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
                    .update(&self.ipfs, self.did.clone(), message, keystore.as_ref())
                    .await?;

                self.get_conversation_mut(document.id(), |conversation_document| {
                    conversation_document.messages.replace(message_document);
                })
                .await?;

                if let Err(e) = tx.send(MessageEventKind::MessageEdited {
                    conversation_id: convo_id,
                    message_id,
                }) {
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
                        .resolve(&self.ipfs, self.did.clone(), keystore.as_ref())
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

                message_document.remove(self.ipfs.clone()).await?;

                self.get_conversation_mut(document.id(), |conversation_document| {
                    conversation_document.messages.remove(&message_document);

                    if let Err(e) = tx.send(MessageEventKind::MessageDeleted {
                        conversation_id: convo_id,
                        message_id,
                    }) {
                        error!("Error broadcasting event: {e}");
                    }
                })
                .await?;
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
                    .resolve(&self.ipfs, self.did.clone(), keystore.as_ref())
                    .await?;

                let event = match state {
                    PinState::Pin => {
                        if message.pinned() {
                            return Ok(false);
                        }
                        *message.pinned_mut() = true;
                        MessageEventKind::MessagePinned {
                            conversation_id: convo_id,
                            message_id,
                        }
                    }
                    PinState::Unpin => {
                        if !message.pinned() {
                            return Ok(false);
                        }
                        *message.pinned_mut() = false;
                        MessageEventKind::MessageUnpinned {
                            conversation_id: convo_id,
                            message_id,
                        }
                    }
                };

                message_document
                    .update(&self.ipfs, self.did.clone(), message, keystore.as_ref())
                    .await?;

                self.get_conversation_mut(document.id(), |conversation_document| {
                    conversation_document.messages.replace(message_document);
                })
                .await?;

                if let Err(e) = tx.send(event) {
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
                    .resolve(&self.ipfs, self.did.clone(), keystore.as_ref())
                    .await?;

                let reactions = message.reactions_mut();

                match state {
                    ReactionState::Add => {
                        match reactions
                            .iter()
                            .position(|reaction| reaction.emoji().eq(&emoji))
                            .and_then(|index| reactions.get_mut(index))
                        {
                            Some(reaction) => {
                                reaction.users_mut().push(sender.clone());
                            }
                            None => {
                                let mut reaction = Reaction::default();
                                reaction.set_emoji(&emoji);
                                reaction.set_users(vec![sender.clone()]);
                                reactions.push(reaction);
                            }
                        };

                        message_document
                            .update(&self.ipfs, self.did.clone(), message, keystore.as_ref())
                            .await?;

                        self.get_conversation_mut(document.id(), |conversation_document| {
                            conversation_document.messages.replace(message_document);
                        })
                        .await?;

                        if let Err(e) = tx.send(MessageEventKind::MessageReactionAdded {
                            conversation_id: convo_id,
                            message_id,
                            did_key: sender,
                            reaction: emoji,
                        }) {
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
                        }
                        message_document
                            .update(&self.ipfs, self.did.clone(), message, keystore.as_ref())
                            .await?;

                        self.get_conversation_mut(document.id(), |conversation_document| {
                            conversation_document.messages.replace(message_document);
                        })
                        .await?;

                        if let Err(e) = tx.send(MessageEventKind::MessageReactionRemoved {
                            conversation_id: convo_id,
                            message_id,
                            did_key: sender,
                            reaction: emoji,
                        }) {
                            error!("Error broadcasting event: {e}");
                        }
                    }
                }
            }
            MessagingEvents::AddRecipient(conversation_id, recipient, list, signature) => {
                if document.recipients.contains(&recipient) {
                    return Err(Error::IdentityExist);
                }

                self.get_conversation_mut(document.id(), |conversation| {
                    conversation.recipients = list;
                    conversation.signature = Some(signature);
                })
                .await?;

                if let Err(e) = tx.send(MessageEventKind::RecipientAdded {
                    conversation_id,
                    recipient,
                }) {
                    error!("Error broadcasting event: {e}");
                }
            }
            MessagingEvents::RemoveRecipient(conversation_id, recipient, list, signature) => {
                if !document.recipients.contains(&recipient) {
                    return Err(Error::IdentityDoesntExist);
                }

                self.get_conversation_mut(document.id(), |conversation| {
                    conversation.recipients = list;
                    conversation.signature = Some(signature);
                })
                .await?;

                if let Err(e) = tx.send(MessageEventKind::RecipientRemoved {
                    conversation_id,
                    recipient,
                }) {
                    error!("Error broadcasting event: {e}");
                }
            }
            MessagingEvents::UpdateConversationName(conversation_id, name, signature) => {
                let name_length = name.trim().len();

                if name_length == 0 || name_length > 255 {
                    return Err(Error::InvalidLength {
                        context: "name".into(),
                        current: name_length,
                        minimum: Some(1),
                        maximum: Some(255),
                    });
                }
                if let Some(current_name) = document.name() {
                    if current_name.eq(&name) {
                        return Ok(false);
                    }
                }

                self.get_conversation_mut(document.id(), |conversation| {
                    conversation.name = Some(name.clone());
                    conversation.signature = Some(signature);
                })
                .await?;

                if let Err(e) = tx.send(MessageEventKind::ConversationNameUpdated {
                    conversation_id,
                    name,
                }) {
                    error!("Error broadcasting event: {e}");
                }
            }
            _ => {}
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
        data: Vec<u8>,
        sent: bool,
    },
}

impl Queue {
    pub fn direct(
        id: Uuid,
        m_id: Option<Uuid>,
        peer: PeerId,
        topic: String,
        data: Vec<u8>,
    ) -> Self {
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
