use std::{collections::HashMap, sync::Arc};

use futures::StreamExt;
use rust_ipfs::Ipfs;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Notify,
};
use uuid::Uuid;
use warp::crypto::DID;

use crate::{
    signaling::{
        ipfs_routes::{call_initiation_route, call_signal_route, peer_signal_route},
        CallSignal, GossipSubSignal, InitiationSignal, PeerSignal,
    },
    store::PeerIdExt,
};

use super::gossipsub_sender::GossipSubSender;

enum GossipSubCmd {
    // unsubscribe from the call and close any webrtc connections
    UnsubscribeCall { call_id: Uuid },
    DisconnectWebrtc { call_id: Uuid },
    // receive call wide broadcasts
    SubscribeCall { call_id: Uuid, group_key: Vec<u8> },
    // webrtc signaling for a peer
    ConnectWebRtc { call_id: Uuid, peer: DID },
    // allow peers to offer calls
    ReceiveCalls { own_id: DID },
}
pub struct GossipSubListener {
    ch: UnboundedSender<GossipSubCmd>,
    // when GossipSubSender gets cloned, NotifyWrapper doesn't get cloned.
    // when NotifyWrapper finally gets dropped, then it's ok to call notify_waiters
    notify: Arc<NotifyWrapper>,
}

struct NotifyWrapper {
    notify: Arc<Notify>,
}

impl Drop for NotifyWrapper {
    fn drop(&mut self) {
        self.notify.notify_waiters();
    }
}

impl GossipSubListener {
    pub fn init(
        ipfs: Ipfs,
        event_ch: UnboundedReceiver<GossipSubCmd>,
        rsp_ch: UnboundedSender<GossipSubSignal>,
        gossipsub_sender: GossipSubSender,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let notify = Arc::new(Notify::new());
        let notify2 = notify.clone();
        tokio::spawn(async move {
            run(ipfs, rx, rsp_ch, gossipsub_sender, notify2).await;
        });
        Self {
            ch: tx,
            notify: Arc::new(NotifyWrapper { notify }),
        }
    }

    pub fn unsubscribe_call(&self, call_id: Uuid) {
        let _ = self.ch.send(GossipSubCmd::UnsubscribeCall { call_id });
    }

    pub fn unsubscribe_webrtc(&self, call_id: Uuid) {
        let _ = self.ch.send(GossipSubCmd::DisconnectWebrtc { call_id });
    }

    pub fn subscribe_call(&self, call_id: Uuid, group_key: Vec<u8>) {
        let _ = self
            .ch
            .send(GossipSubCmd::SubscribeCall { call_id, group_key });
    }

    pub fn connect_webrtc(&self, call_id: Uuid, peer: DID) {
        let _ = self.ch.send(GossipSubCmd::ConnectWebRtc { call_id, peer });
    }

    pub fn receive_calls(&self, own_id: DID) {
        let _ = self.ch.send(GossipSubCmd::ReceiveCalls { own_id });
    }
}

async fn run(
    ipfs: Ipfs,
    mut ch: UnboundedReceiver<GossipSubCmd>,
    tx: UnboundedSender<GossipSubSignal>,
    gossipsub_sender: GossipSubSender,
    notify: Arc<Notify>,
) {
    // for tracking webrtc subscriptions
    let mut current_call: Option<Uuid> = None;
    let mut connected_peers: HashMap<DID, Arc<Notify>> = HashMap::new();
    let mut subscribed_calls: HashMap<Uuid, Arc<Notify>> = HashMap::new();

    let call_signal_notify = Arc::new(Notify::new());
    let call_offer_notify = Arc::new(Notify::new());
    loop {
        tokio::select! {
            opt = ch.recv() => match opt {
                Some(cmd) => match cmd {
                    GossipSubCmd::UnsubscribeCall { call_id } => {
                        if let Some(call) = subscribed_calls.remove(&call_id) {
                            call.notify_waiters();
                        }
                        if matches!(current_call.as_ref(), Some(&call_id)) {
                            let _ = current_call.take();
                            for (_peer_id, notify) in connected_peers.drain() {
                                notify.notify_waiters();
                            }
                        }
                    }
                    GossipSubCmd::DisconnectWebrtc { call_id } => {
                        if matches!(current_call.as_ref(), Some(&call_id)) {
                            for (_peer_id, notify) in connected_peers.drain() {
                                notify.notify_waiters();
                            }
                        }
                    }
                    GossipSubCmd::SubscribeCall { call_id, group_key } => {
                        let notify = Arc::new(Notify::new());
                        if let Some(prev) = subscribed_calls.insert(call_id, notify.clone()) {
                            prev.notify_waiters();
                        }

                        let mut call_signal_stream = match ipfs
                            .pubsub_subscribe(call_signal_route(&call_id))
                            .await
                        {
                            Ok(s) => s,
                            Err(e) => {
                                log::error!("failed to subscribe to call signal stream: {e}");
                                continue;
                            }
                        };

                        let ch = tx.clone();
                        let gossipsub_sender = gossipsub_sender.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select!{
                                    _ = notify.notified() => {
                                        log::debug!("call signal stream terminated by notify");
                                        break;
                                    }
                                    opt = call_signal_stream.next() => match opt {
                                        Some(msg) => {
                                            let sender = match msg.source.and_then(|s| s.to_did().ok()) {
                                                Some(id) => id,
                                                None => {
                                                    log::error!("msg received without source");
                                                    continue
                                                }
                                            };
                                            match gossipsub_sender.decode_signal_aes::<CallSignal>(group_key.clone(), msg.data.clone()).await {
                                                Ok(msg) => {
                                                    let _ = ch.send(GossipSubSignal::Call{
                                                        sender,
                                                        call_id: call_id.clone(),
                                                        signal: msg
                                                    });
                                                },
                                                Err(e) => {
                                                    log::error!("failed to decode call signal: {e}");
                                                }
                                            };
                                        }
                                        None => {
                                            log::debug!("call signal stream terminated!");
                                            break;
                                        }
                                    }
                                };
                            }
                        });
                    },
                    GossipSubCmd::ConnectWebRtc { call_id, peer } => {
                        if !matches!(current_call.as_ref(), Some(&call_id)) {
                            if current_call.is_some() {
                                for (_peer_id, notify) in connected_peers.drain() {
                                    notify.notify_waiters();
                                }
                            }
                            current_call.replace(call_id);
                        }
                        let notify = Arc::new(Notify::new());
                        if let Some(prev) = connected_peers.insert(peer.clone(), notify.clone()) {
                            prev.notify_waiters();
                        }

                        let mut peer_signal_stream = match ipfs
                            .pubsub_subscribe(peer_signal_route(&peer, &call_id))
                            .await
                        {
                            Ok(s) => s,
                            Err(e) => {
                                log::error!("failed to subscribe to peer signal stream: {e}");
                                continue;
                            }
                        };
                        let ch = tx.clone();
                        let gossipsub_sender = gossipsub_sender.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select!{
                                    _ = notify.notified() => {
                                        log::debug!("peer signal stream terminated by notify");
                                        break;
                                    }
                                    opt = peer_signal_stream.next() => match opt {
                                        Some(msg) => {
                                            let sender = match msg.source.and_then(|s| s.to_did().ok()) {
                                                Some(id) => id,
                                                None => {
                                                    log::error!("msg received without source");
                                                    continue
                                                }
                                            };
                                            match gossipsub_sender.decode_signal_ecdh::<PeerSignal>(sender.clone(), msg.data.clone()).await {
                                                Ok(msg) => {
                                                    let _ = ch.send(GossipSubSignal::Peer {
                                                        sender,
                                                        call_id: call_id.clone(),
                                                        signal: msg
                                                    });
                                                },
                                                Err(e) => {
                                                    log::error!("failed to decode peer signal: {e}");
                                                }
                                            };
                                        }
                                        None => {
                                            log::debug!("peer signal stream terminated!");
                                            break;
                                        }
                                    }
                                };
                            }
                        });
                    },
                    GossipSubCmd::ReceiveCalls { own_id } => {
                        call_offer_notify.notify_waiters();
                        let mut call_offer_stream = match ipfs
                            .pubsub_subscribe(call_initiation_route(&own_id))
                            .await
                        {
                            Ok(s) => s,
                            Err(e) => {
                                log::error!("failed to subscribe to call offer stream: {e}");
                                continue;
                            }
                        };
                        let ch = tx.clone();
                        let notify = call_offer_notify.clone();
                        let gossipsub_sender = gossipsub_sender.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select!{
                                    _ = notify.notified() => {
                                        log::debug!("call offer stream terminated by notify");
                                        break;
                                    }
                                    opt = call_offer_stream.next() => match opt {
                                        Some(msg) => {
                                            let sender = match msg.source.and_then(|s| s.to_did().ok()) {
                                                Some(id) => id,
                                                None => {
                                                    log::error!("msg received without source");
                                                    continue
                                                }
                                            };
                                            match gossipsub_sender.decode_signal_ecdh::<InitiationSignal>(sender.clone(), msg.data.clone()).await {
                                                Ok(msg) => {
                                                    let _ = ch.send(GossipSubSignal::Initiation{
                                                        sender,
                                                        signal: msg
                                                    });
                                                },
                                                Err(e) => {
                                                    log::error!("failed to decode call offer: {e}");
                                                }
                                            };
                                        }
                                        None => {
                                            log::debug!("call offer stream terminated!");
                                            break;
                                        }
                                    }
                                };
                            }
                        });
                    },
                }
                None => {
                    log::debug!("GossipSubListener channel closed");
                    break;
                }
            },
            _ = notify.notified() => {
                log::debug!("GossipSubListener terminated");
                break;
            }
        }

        for (_peer_id, notify) in connected_peers.drain() {
            notify.notify_waiters();
        }
        call_signal_notify.notify_waiters();
        call_offer_notify.notify_waiters();
    }
}
