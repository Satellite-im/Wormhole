use futures::channel::oneshot;
use futures::StreamExt;

use super::signaling::{
    self, ipfs_routes, CallSignal, GossipSubSignal, InitiationSignal, PeerSignal,
};

use std::sync::Arc;
use tokio::sync::{
    broadcast,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Notify,
};
use uuid::Uuid;
use warp::{
    blink::{BlinkEventKind, CallInfo, CallState},
    error::Error,
};
use webrtc::{
    rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
    track::track_local::track_local_static_rtp::TrackLocalStaticRTP,
};

use crate::{
    host_media::{self, audio::AudioCodec, mp4_logger::Mp4LoggerConfig},
    simple_webrtc::{self, events::WebRtcEventStream, MediaSourceId},
};

use super::{
    data::{CallDataMap, NotifyWrapper},
    gossipsub_listener::GossipSubListener,
    gossipsub_sender::GossipSubSender,
};

enum Cmd {
    OfferCall {
        call_info: CallInfo,
        rsp: oneshot::Sender<Result<(), Error>>,
    },
    AnswerCall {
        call_id: Uuid,
        rsp: oneshot::Sender<Result<(), Error>>,
    },
    AddMediaSource {
        source_id: MediaSourceId,
        codec: RTCRtpCodecCapability,
        rsp: oneshot::Sender<anyhow::Result<Arc<TrackLocalStaticRTP>>>,
    },
    RemoveMediaSource {
        source_id: MediaSourceId,
    },
    GetCallInfo {
        call_id: Uuid,
        rsp: oneshot::Sender<Option<CallInfo>>,
    },
    LeaveCall {
        call_id: Option<Uuid>,
    },
    MuteSelf,
    UnmuteSelf,
    SilenceCall,
    UnsilenceCall,
    GetPendingCalls {
        rsp: oneshot::Sender<Vec<CallInfo>>,
    },
    GetActiveCallInfo {
        rsp: oneshot::Sender<Option<CallInfo>>,
    },
    GetActiveCallState {
        rsp: oneshot::Sender<Option<CallState>>,
    },
    RecordCall {
        output_dir: String,
        rsp: oneshot::Sender<Result<(), Error>>,
    },
    StopRecording {
        rsp: oneshot::Sender<Result<(), Error>>,
    },
}

#[derive(Clone)]
pub struct BlinkController {
    ch: UnboundedSender<Cmd>,
    notify: Arc<NotifyWrapper>,
}

pub struct Args {
    pub webrtc_controller: simple_webrtc::Controller,
    pub webrtc_event_stream: WebRtcEventStream,
    pub gossipsub_sender: GossipSubSender,
    pub gossipsub_listener: GossipSubListener,
    pub signal_rx: UnboundedReceiver<GossipSubSignal>,
    pub ui_event_ch: broadcast::Sender<BlinkEventKind>,
}

impl BlinkController {
    pub fn new(args: Args) -> Self {
        let Args {
            webrtc_controller,
            webrtc_event_stream,
            gossipsub_sender,
            gossipsub_listener,
            signal_rx,
            ui_event_ch,
        } = args;

        let (tx, cmd_rx) = mpsc::unbounded_channel();
        let notify = Arc::new(Notify::new());
        let notify2 = notify.clone();
        tokio::spawn(async move {
            run(
                webrtc_controller,
                webrtc_event_stream,
                gossipsub_sender,
                gossipsub_listener,
                signal_rx,
                ui_event_ch,
                cmd_rx,
                notify2,
            )
            .await;
        });
        Self {
            ch: tx,
            notify: Arc::new(NotifyWrapper { notify }),
        }
    }

    pub async fn offer_call(&self, call_info: CallInfo) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.ch.send(Cmd::OfferCall { call_info, rsp: tx })?;
        rx.await??;
        Ok(())
    }

    pub async fn answer_call(&self, call_id: Uuid) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.ch
            .send(Cmd::AnswerCall { call_id, rsp: tx })
            .map_err(|x| Error::OtherWithContext(x.to_string()))?;
        rx.await
            .map_err(|x| Error::FailedToSendSignal(x.to_string()))?
    }

    pub async fn add_media_source(
        &self,
        source_id: MediaSourceId,
        codec: RTCRtpCodecCapability,
    ) -> anyhow::Result<Arc<TrackLocalStaticRTP>> {
        let (tx, rx) = oneshot::channel();
        self.ch.send(Cmd::AddMediaSource {
            source_id,
            codec,
            rsp: tx,
        })?;
        rx.await?
    }

    pub fn remove_media_source(&self, source_id: MediaSourceId) -> anyhow::Result<()> {
        self.ch.send(Cmd::RemoveMediaSource { source_id })?;
        Ok(())
    }

    pub async fn get_call_info(&self, call_id: Uuid) -> Option<CallInfo> {
        let (tx, rx) = oneshot::channel();
        self.ch.send(Cmd::GetCallInfo { call_id, rsp: tx }).ok()?;
        rx.await.ok()?
    }

    pub fn leave_call(&self, call_id: Option<Uuid>) -> anyhow::Result<()> {
        self.ch.send(Cmd::LeaveCall { call_id })?;
        Ok(())
    }

    pub fn mute_self(&self) -> anyhow::Result<()> {
        self.ch.send(Cmd::MuteSelf)?;
        Ok(())
    }

    pub fn unmute_self(&self) -> anyhow::Result<()> {
        self.ch.send(Cmd::UnmuteSelf)?;
        Ok(())
    }
    pub fn silence_call(&self) -> anyhow::Result<()> {
        self.ch.send(Cmd::SilenceCall)?;
        Ok(())
    }
    pub fn unsilence_call(&self) -> anyhow::Result<()> {
        self.ch.send(Cmd::UnsilenceCall)?;
        Ok(())
    }

    pub async fn get_pending_calls(&self) -> Result<Vec<CallInfo>, Error> {
        let (tx, rx) = oneshot::channel();
        self.ch
            .send(Cmd::GetPendingCalls { rsp: tx })
            .map_err(|x| Error::OtherWithContext(x.to_string()))?;
        rx.await.map_err(|x| Error::OtherWithContext(x.to_string()))
    }

    pub async fn get_active_call_info(&self) -> Result<Option<CallInfo>, Error> {
        let (tx, rx) = oneshot::channel();
        self.ch
            .send(Cmd::GetActiveCallInfo { rsp: tx })
            .map_err(|x| Error::OtherWithContext(x.to_string()))?;
        rx.await.map_err(|x| Error::OtherWithContext(x.to_string()))
    }

    pub async fn get_active_call_state(&self) -> Result<Option<CallState>, Error> {
        let (tx, rx) = oneshot::channel();
        self.ch
            .send(Cmd::GetActiveCallState { rsp: tx })
            .map_err(|x| Error::OtherWithContext(x.to_string()))?;
        rx.await.map_err(|x| Error::OtherWithContext(x.to_string()))
    }
    pub async fn record_call(&self, output_dir: String) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.ch
            .send(Cmd::RecordCall {
                output_dir,
                rsp: tx,
            })
            .map_err(|x| Error::OtherWithContext(x.to_string()))?;
        rx.await
            .map_err(|x| Error::OtherWithContext(x.to_string()))?
    }

    pub async fn stop_recording(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.ch
            .send(Cmd::StopRecording { rsp: tx })
            .map_err(|x| Error::OtherWithContext(x.to_string()))?;
        rx.await
            .map_err(|x| Error::OtherWithContext(x.to_string()))?
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    mut webrtc_controller: simple_webrtc::Controller,
    mut webrtc_event_stream: WebRtcEventStream,
    gossipsub_sender: GossipSubSender,
    gossipsub_listener: GossipSubListener,
    mut signal_rx: UnboundedReceiver<GossipSubSignal>,
    ui_event_ch: broadcast::Sender<BlinkEventKind>,
    mut cmd_rx: UnboundedReceiver<Cmd>,
    notify: Arc<Notify>,
) {
    let own_id = {
        let notify2 = notify.clone();
        let fut = gossipsub_sender.get_own_id();
        tokio::select! {
            _ = notify2.notified() => {
                log::debug!("quitting blink event handler");
                return;
            }
            r = fut => {
                match r {
                    Ok(r) => r,
                    Err(e) => {
                        log::debug!("failed to get own id. quitting blink event handler: {e}");
                        return;
                    }
                }
            }
        }
    };
    // prevent accidental moves
    let own_id = &own_id;

    let mut call_data_map = CallDataMap::new(own_id.clone());
    let mut active_call: Option<Uuid> = None;

    loop {
        tokio::select! {
            _ = notify.notified() => {
                log::debug!("quitting blink event handler");
                break;
            },
            opt = cmd_rx.recv() => {
                let cmd = match opt {
                    Some(r) => r,
                    None => {
                        log::debug!("blink handler cmd_rx channel is closed. quitting");
                        break;
                    }
                };
                match cmd {
                    Cmd::OfferCall { call_info, rsp } => {
                        let prev_active = active_call.unwrap_or_default();
                        if let Some(data) = call_data_map.map.get_mut(&prev_active) {
                            data.state.reset_self();
                        }
                        if active_call.replace(call_info.call_id()).is_some() {
                            let _ = webrtc_controller.deinit().await;
                            host_media::reset().await;
                        }
                        call_data_map.add_call(call_info.clone(), own_id);

                        // automatically add an audio track
                        let webrtc_codec = AudioCodec::default();
                        let rtc_rtp_codec: RTCRtpCodecCapability = RTCRtpCodecCapability {
                            mime_type: webrtc_codec.mime_type(),
                            clock_rate: webrtc_codec.sample_rate(),
                            channels: 1,
                            ..Default::default()
                        };
                        match webrtc_controller.add_media_source(host_media::AUDIO_SOURCE_ID.into(), rtc_rtp_codec).await {
                            Ok(track) => {
                                match host_media::create_audio_source_track(
                                    own_id.clone(),
                                    ui_event_ch.clone(),
                                    track,
                                    webrtc_codec).await
                                {
                                    Ok(_) => {
                                        gossipsub_listener
                                            .subscribe_call(call_info.call_id(), call_info.group_key());
                                        gossipsub_listener
                                            .connect_webrtc(call_info.call_id(), own_id.clone());

                                        // todo: resend periodically. perhaps somewhere else
                                        let mut participants = call_info.participants();
                                        participants.retain(|x| x != own_id);
                                        for dest in participants {
                                            let topic = ipfs_routes::call_initiation_route(&dest);
                                            let signal = InitiationSignal::Offer {
                                                call_info: call_info.clone(),
                                            };

                                            if let Err(e) = gossipsub_sender.send_signal_ecdh(dest, signal, topic) {
                                                log::error!("failed to send signal: {e}");
                                            }
                                        }
                                        let _ = rsp.send(Ok(()));
                                    }
                                    Err(e) => {
                                        let _ = webrtc_controller.remove_media_source(host_media::AUDIO_SOURCE_ID.into()).await;
                                        let _ = rsp.send(Err(e));
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = rsp.send(Err(Error::OtherWithContext(e.to_string())));
                            }
                        }
                    },
                    Cmd::AnswerCall { call_id, rsp } => {
                        let call_info = match call_data_map.get_call_info(call_id) {
                            Some(r) => r,
                            None => {
                                let _ = rsp.send(Err(Error::CallNotFound));
                                continue;
                            }
                        };

                        let prev_active = active_call.unwrap_or_default();
                        if let Some(data) = call_data_map.map.get_mut(&prev_active) {
                            data.state.reset_self();
                        }
                        if active_call.replace(call_id).is_some() {
                            let _ = webrtc_controller.deinit().await;
                            host_media::reset().await;
                        }

                        // automatically add an audio track
                        let webrtc_codec = AudioCodec::default();
                        let rtc_rtp_codec: RTCRtpCodecCapability = RTCRtpCodecCapability {
                            mime_type: webrtc_codec.mime_type(),
                            clock_rate: webrtc_codec.sample_rate(),
                            channels: 1,
                            ..Default::default()
                        };
                        match webrtc_controller.add_media_source(host_media::AUDIO_SOURCE_ID.into(), rtc_rtp_codec).await {
                            Ok(track) => {
                                let r = host_media::create_audio_source_track(
                                    own_id.clone(),
                                    ui_event_ch.clone(),
                                    track,
                                    webrtc_codec).await;
                                match r {
                                    Ok(_) => {
                                        gossipsub_listener.subscribe_call(call_id, call_info.group_key());
                                        gossipsub_listener.connect_webrtc(call_id, own_id.clone());
                                        let topic = ipfs_routes::call_signal_route(&call_id);

                                        // todo? periodically re-send join signals. perhaps somewhere else
                                        let signal = CallSignal::Join;
                                        if let Err(e) =
                                            gossipsub_sender
                                            .send_signal_aes(call_info.group_key(), signal, topic)
                                        {
                                            let _ = rsp.send(Err(Error::FailedToSendSignal(e.to_string())));
                                        } else {
                                            let _ = rsp.send(Ok(()));
                                        }
                                    }
                                    Err(e) => {
                                        let _ = webrtc_controller.remove_media_source(host_media::AUDIO_SOURCE_ID.into()).await;
                                        let _ = rsp.send(Err(e));
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = rsp.send(Err(e.into()));
                            }
                        }
                    }
                    Cmd::AddMediaSource { source_id, codec, rsp } => {
                        let r = webrtc_controller.add_media_source(source_id, codec).await;
                        let _ = rsp.send(r);
                    },
                    Cmd::GetCallInfo { call_id, rsp } => {
                        let _ = rsp.send(call_data_map.get_call_info(call_id));
                    }
                    Cmd::RemoveMediaSource { source_id } => {
                        let _ = webrtc_controller.remove_media_source(source_id).await;
                    },
                    Cmd::LeaveCall { call_id } => {
                        let call_id = call_id.unwrap_or(active_call.unwrap_or_default());
                        match call_data_map.get_call_info(call_id) {
                            Some(info) => {
                                let topic = ipfs_routes::call_signal_route(&call_id);
                                let signal = CallSignal::Leave;
                                if let Err(e) = gossipsub_sender
                                    .send_signal_aes(info.group_key(), signal, topic)
                                {
                                    log::error!("failed to send signal: {e}");
                                }
                            }
                            None => {
                                log::error!("failed to leave call - not found");
                            }
                        }
                        if active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() {
                            call_data_map.leave_call(call_id);
                            let _ = active_call.take();
                            let _ = webrtc_controller.deinit().await;
                            host_media::reset().await;
                            if let Err(e) = ui_event_ch.send(BlinkEventKind::CallTerminated { call_id }) {
                                log::error!("failed to send CallTerminated Event: {e}");
                            }
                        }
                    },
                    Cmd::MuteSelf => {
                        let call_id = active_call.unwrap_or_default();
                        if let Some(data) = call_data_map.map.get_mut(&call_id) {
                            data.state.set_self_muted(true);
                            let topic = ipfs_routes::call_signal_route(&call_id);
                            let signal = CallSignal::Muted;
                            if let Err(e) =
                                gossipsub_sender
                                    .send_signal_aes(data.info.group_key(), signal, topic)
                            {
                                log::error!("failed to send signal: {e}");
                            } else {
                                log::debug!("sent signal to mute self");
                            }
                        }
                    }
                    Cmd::UnmuteSelf => {
                        let call_id = active_call.unwrap_or_default();
                        if let Some(data) = call_data_map.map.get_mut(&call_id) {
                            data.state.set_self_muted(false);
                            let topic = ipfs_routes::call_signal_route(&call_id);
                            let signal = CallSignal::Unmuted;
                            if let Err(e) =
                                gossipsub_sender
                                    .send_signal_aes(data.info.group_key(), signal, topic)
                            {
                                log::error!("failed to send signal: {e}");
                            } else {
                                log::debug!("sent signal to unmute self");
                            }
                        }
                    }
                    Cmd::SilenceCall => {
                        let call_id = active_call.unwrap_or_default();
                        if let Some(data) = call_data_map.map.get_mut(&call_id) {
                            if let Err(e) = host_media::deafen().await {
                                log::error!("{e}");
                            }
                            data.state.set_deafened(own_id, true);
                            let topic = ipfs_routes::call_signal_route(&call_id);
                            let signal = CallSignal::Deafened;
                            if let Err(e) =
                                gossipsub_sender
                                    .send_signal_aes(data.info.group_key(), signal, topic)
                            {
                                log::error!("failed to send signal: {e}");
                            }
                        }
                    }
                    Cmd::UnsilenceCall => {
                        let call_id = active_call.unwrap_or_default();
                        if let Some(data) = call_data_map.map.get_mut(&call_id) {
                            if let Err(e) = host_media::undeafen().await {
                                log::error!("{e}");
                            }
                            data.state.set_deafened(own_id, false);
                            let topic = ipfs_routes::call_signal_route(&call_id);
                            let signal = CallSignal::Undeafened;
                            if let Err(e) =
                                gossipsub_sender
                                    .send_signal_aes(data.info.group_key(), signal, topic)
                            {
                                log::error!("failed to send signal: {e}");
                            }
                        }
                    }
                    Cmd::GetPendingCalls { rsp } => {
                        let _ = rsp.send(call_data_map.get_pending_calls());
                    }
                    Cmd::GetActiveCallState { rsp } => {
                        if active_call.is_none() {
                            let _ = rsp.send(None);
                        } else {
                            let _ = rsp.send(call_data_map.get_call_state(active_call.unwrap_or_default()));
                        }
                    }
                    Cmd::GetActiveCallInfo { rsp } => {
                        if active_call.is_none() {
                            let _ = rsp.send(None);
                        } else {
                            let _ = rsp.send(call_data_map.get_call_info(active_call.unwrap_or_default()));
                        }
                    }
                    Cmd::RecordCall { output_dir, rsp } => {
                        let call_id = active_call.unwrap_or_default();
                        if let Some(data) = call_data_map.map.get_mut(&call_id) {
                            let info = data.get_info();
                            match
                                 host_media::init_recording(Mp4LoggerConfig {
                                    call_id: info.call_id(),
                                    participants: info.participants(),
                                    audio_codec: AudioCodec::default(),
                                    log_path: output_dir.into(),
                                })
                                .await
                            {
                                Ok(_) => {
                                    data.state.set_self_recording(true);
                                    let topic = ipfs_routes::call_signal_route(&info.call_id());
                                    let signal = CallSignal::Recording;
                                    if let Err(e) =
                                        gossipsub_sender
                                            .send_signal_aes(info.group_key(), signal, topic)
                                    {
                                        log::error!("failed to send signal: {e}");
                                    }
                                    let _ = rsp.send(Ok(()));
                                }
                                Err(e) => {
                                    let _ = rsp.send(Err(Error::OtherWithContext(e.to_string())));
                                }
                            }
                        } else {
                            let _ = rsp.send(Err(Error::CallNotInProgress));
                        }
                    }
                    Cmd::StopRecording { rsp } => {
                        let call_id = active_call.unwrap_or_default();
                        if let Some(data) = call_data_map.map.get_mut(&call_id) {
                            let info = data.get_info();
                            match
                                 host_media::pause_recording()
                                .await
                            {
                                Ok(_) => {
                                    data.state.set_self_recording(false);
                                    let topic = ipfs_routes::call_signal_route(&info.call_id());
                                    let signal = CallSignal::NotRecording;
                                    if let Err(e) =
                                        gossipsub_sender
                                            .send_signal_aes(info.group_key(), signal, topic)
                                    {
                                        log::error!("failed to send signal: {e}");
                                    }
                                    let _ = rsp.send(Ok(()));
                                }
                                Err(e) => {
                                    let _ = rsp.send(Err(Error::OtherWithContext(e.to_string())));
                                }
                            }
                        } else {
                            let _ = rsp.send(Err(Error::CallNotInProgress));
                        }
                    }
                }
            },
            opt = signal_rx.recv() => {
                let cmd = match opt {
                    Some(r) => r,
                    None => {
                        log::debug!("blink handler signal_rx channel is closed. quitting");
                        break;
                    }
                };
                match cmd {
                    GossipSubSignal::Peer { sender, call_id, signal } => match *signal {
                        _ if !active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() => {
                            log::debug!("received webrtc signal for non-active call");
                            continue;
                        }
                        _ if !call_data_map.participant_in_call(call_id, &sender) => {
                            log::debug!("received signal from someone who isn't part of the call");
                            continue;
                        }
                        signaling::PeerSignal::Ice(ice) => {
                            if let Err(e) = webrtc_controller.recv_ice(&sender, ice).await {
                                log::error!("failed to recv_ice {}", e);
                            }
                        },
                        signaling::PeerSignal::Sdp(sdp) => {
                            log::debug!("received signal: SDP");
                            if let Err(e) = webrtc_controller.recv_sdp(&sender, sdp).await {
                                log::error!("failed to recv_sdp: {}", e);
                            }
                        },
                        signaling::PeerSignal::Dial(sdp) => {
                            log::debug!("received signal: Dial");
                            // emits the SDP Event, which is sent to the peer via the SDP signal
                            if let Err(e) = webrtc_controller.accept_call(&sender, sdp).await {
                                log::error!("failed to accept_call: {}", e);
                            }
                        },
                    },
                    GossipSubSignal::Call { sender, call_id, signal } => match signal {
                        _ if !call_data_map.participant_in_call(call_id, &sender) => {
                            log::debug!("received signal from someone who isn't part of the call");
                            continue;
                        }
                        signaling::CallSignal::Join => {
                            call_data_map.add_participant(call_id, &sender);

                            if active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() {
                                if let Err(e) = webrtc_controller.dial(&sender).await {
                                    log::error!("failed to dial peer: {e}");
                                    continue;
                                }
                                if let Err(e) = ui_event_ch.send(BlinkEventKind::ParticipantJoined { call_id, peer_id: sender }) {
                                    log::error!("failed to send ParticipantJoined Event: {e}");
                                }
                            }
                        },
                        signaling::CallSignal::Leave => {
                            call_data_map.remove_participant(call_id, &sender);
                            let is_call_empty = call_data_map.call_empty(call_id);

                            if active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() {
                                webrtc_controller.hang_up(&sender).await;
                                if let Err(e) = ui_event_ch.send(BlinkEventKind::ParticipantLeft { call_id, peer_id: sender }) {
                                    log::error!("failed to send ParticipantLeft event: {e}");
                                }
                            } else if is_call_empty {
                                call_data_map.remove_call(call_id);
                                if let Err(e) = ui_event_ch.send(BlinkEventKind::CallCancelled { call_id }) {
                                    log::error!("failed to send CallCancelled event: {e}");
                                }
                            }
                        },
                        signaling::CallSignal::Muted => {
                            call_data_map.set_muted(call_id, &sender, true);

                            if active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() {
                                if let Err(e) = ui_event_ch.send(BlinkEventKind::ParticipantMuted { peer_id: sender }) {
                                    log::error!("failed to send ParticipantMuted event: {e}");
                                }
                            }
                        },
                        signaling::CallSignal::Unmuted => {
                            call_data_map.set_muted(call_id, &sender, false);

                            if active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() {
                                if let Err(e) = ui_event_ch.send(BlinkEventKind::ParticipantUnmuted { peer_id: sender }) {
                                    log::error!("failed to send ParticipantUnmuted event: {e}");
                                }
                            }
                        },
                        signaling::CallSignal::Deafened => {
                            call_data_map.set_deafened(call_id, &sender, true);

                            if active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() {
                                if let Err(e) = ui_event_ch.send(BlinkEventKind::ParticipantDeafened { peer_id: sender }) {
                                    log::error!("failed to send ParticipantDeafened event: {e}");
                                }
                            }
                        },
                        signaling::CallSignal::Undeafened => {
                            call_data_map.set_deafened(call_id, &sender, false);

                            if active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() {
                                if let Err(e) = ui_event_ch.send(BlinkEventKind::ParticipantUndeafened { peer_id: sender }) {
                                    log::error!("failed to send ParticipantUndeafened event: {e}");
                                }
                            }
                        },
                        signaling::CallSignal::Recording => {
                            call_data_map.set_recording(call_id, &sender, true);
                            if active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() {
                                if let Err(e) = ui_event_ch.send(BlinkEventKind::ParticipantRecording { peer_id: sender }) {
                                    log::error!("failed to send ParticipantRecording event: {e}");
                                }
                            }
                        }
                        signaling::CallSignal::NotRecording => {
                            call_data_map.set_recording(call_id, &sender, false);
                            if active_call.as_ref().map(|x| x == &call_id).unwrap_or_default() {
                                if let Err(e) = ui_event_ch.send(BlinkEventKind::ParticipantNotRecording { peer_id: sender }) {
                                    log::error!("failed to send ParticipantNotRecording event: {e}");
                                }
                            }
                        }
                    },
                    GossipSubSignal::Initiation { sender, signal } => match signal {
                        signaling::InitiationSignal::Offer { call_info } => {
                            let call_id = call_info.call_id();
                            let conversation_id = call_info.conversation_id();
                            let participants = call_info.participants();
                            call_data_map.add_call(call_info, &sender);

                            if let Err(e) = ui_event_ch.send(BlinkEventKind::IncomingCall { call_id, conversation_id, sender, participants }) {
                                log::error!("failed to send IncomingCall event: {e}");
                            }
                        },
                    },
                }
            }
            opt = webrtc_event_stream.next() => {
                let event = match opt {
                    Some(r) => r,
                    None => {
                        log::debug!("webrtc_event_stream closed!");
                        // todo: get new webrtc controller or something
                        continue;
                    }
                };

                match event {
                    simple_webrtc::events::EmittedEvents::Ice { dest, candidate } => {
                        let topic = ipfs_routes::peer_signal_route(&dest, &active_call.unwrap_or_default());
                        let signal = PeerSignal::Ice(*candidate);
                        if let Err(e) = gossipsub_sender.send_signal_ecdh(dest, signal, topic) {
                            log::error!("failed to send signal: {e}");
                        }
                    },
                    simple_webrtc::events::EmittedEvents::Connected { peer } => {
                        let ac = active_call.unwrap_or_default();
                        if call_data_map.contains_participant(ac, &peer) {
                             call_data_map.add_participant(ac, &peer);
                        } else {
                             log::warn!("webrtc controller connected to a peer who wasn't in the list for the active call");
                            webrtc_controller.hang_up(&peer).await;
                        }
                    },
                    simple_webrtc::events::EmittedEvents::Disconnected { peer }
                    | simple_webrtc::events::EmittedEvents::ConnectionFailed { peer } => {
                        let ac = active_call.unwrap_or_default();
                        call_data_map.remove_participant(ac, &peer);

                        if let Err(e) = host_media::remove_sink_track(peer.clone()).await {
                            log::error!("failed to send media_track command: {e}");
                        }
                        webrtc_controller.hang_up(&peer).await;
                    },
                    simple_webrtc::events::EmittedEvents::ConnectionClosed { peer: _ } => {
                        // todo
                    },
                    simple_webrtc::events::EmittedEvents::Sdp { dest, sdp } => {
                        let topic = ipfs_routes::peer_signal_route(&dest, &active_call.unwrap_or_default());
                        let signal = PeerSignal::Sdp(*sdp);
                        if let Err(e) = gossipsub_sender.send_signal_ecdh(dest, signal, topic) {
                            log::error!("failed to send signal: {e}");
                        }
                    },
                    simple_webrtc::events::EmittedEvents::CallInitiated { dest, sdp } => {
                        let topic = ipfs_routes::peer_signal_route(&dest, &active_call.unwrap_or_default());
                        let signal = PeerSignal::Dial(*sdp);
                        if let Err(e) = gossipsub_sender.send_signal_ecdh(dest, signal, topic) {
                            log::error!("failed to send signal: {e}");
                        }
                    },
                    simple_webrtc::events::EmittedEvents::TrackAdded { peer, track } => {
                        if let Err(e) =   host_media::create_audio_sink_track(peer.clone(), ui_event_ch.clone(), track, AudioCodec::default()).await {
                            log::error!("failed to send media_track command: {e}");
                        }
                    },
                }
            }
        }
    }
}
