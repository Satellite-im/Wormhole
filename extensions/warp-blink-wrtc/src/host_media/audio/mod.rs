use anyhow::{bail, Result};
use cpal::traits::{DeviceTrait, HostTrait};
use futures::stream::BoxStream;
use std::sync::Arc;
use warp::blink::{self, MimeType};
use warp::crypto::DID;
use webrtc::track::{
    track_local::track_local_static_rtp::TrackLocalStaticRTP, track_remote::TrackRemote,
};

pub mod loudness;
mod opus;
pub mod speech;

pub use self::opus::sink::OpusSink;
pub use self::opus::source::OpusSource;

#[derive(Clone)]
pub enum AudioEvent {
    UserTalking(DID),
    PacketLoss(DID),
}

pub struct AudioEventStream(pub BoxStream<'static, AudioEvent>);

// stores the TrackRemote at least
pub trait SourceTrack {
    fn init(
        input_device: &cpal::Device,
        track: Arc<TrackLocalStaticRTP>,
        webrtc_codec: blink::AudioCodec,
        source_codec: blink::AudioCodec,
    ) -> Result<Self>
    where
        Self: Sized;

    fn play(&self) -> Result<()>;
    fn pause(&self) -> Result<()>;
    fn record(&mut self, output_file_name: &str) -> Result<()>;
    fn stop_recording(&mut self) -> Result<()>;
    // should not require RTP renegotiation
    fn change_input_device(&mut self, input_device: &cpal::Device) -> Result<()>;
}

// stores the TrackRemote at least
pub trait SinkTrack {
    fn init(
        peer_id: DID,
        output_device: &cpal::Device,
        track: Arc<TrackRemote>,
        webrtc_codec: blink::AudioCodec,
        sink_codec: blink::AudioCodec,
    ) -> Result<Self>
    where
        Self: Sized;
    fn change_output_device(&mut self, output_device: &cpal::Device) -> anyhow::Result<()>;
    fn play(&self) -> Result<()>;
    fn pause(&self) -> Result<()>;

    fn record(&mut self, output_file_name: &str) -> Result<()>;
    fn stop_recording(&mut self) -> Result<()>;

    fn get_event_stream(&mut self) -> Result<AudioEventStream>;
}

/// Uses the MIME type from codec to determine which implementation of SourceTrack to create
pub fn create_source_track(
    input_device: &cpal::Device,
    track: Arc<TrackLocalStaticRTP>,
    webrtc_codec: blink::AudioCodec,
    source_codec: blink::AudioCodec,
) -> Result<Box<dyn SourceTrack>> {
    if webrtc_codec.mime_type() != source_codec.mime_type() {
        bail!("mime types don't match");
    }
    match MimeType::try_from(webrtc_codec.mime_type().as_str())? {
        MimeType::OPUS => Ok(Box::new(OpusSource::init(
            input_device,
            track,
            webrtc_codec,
            source_codec,
        )?)),
        _ => {
            bail!("unhandled mime type: {}", &webrtc_codec.mime_type());
        }
    }
}

/// Uses the MIME type from codec to determine which implementation of SinkTrack to create
pub fn create_sink_track(
    peer_id: DID,
    output_device: &cpal::Device,
    track: Arc<TrackRemote>,
    webrtc_codec: blink::AudioCodec,
    sink_codec: blink::AudioCodec,
) -> Result<Box<dyn SinkTrack>> {
    if webrtc_codec.mime_type() != sink_codec.mime_type() {
        bail!("mime types don't match");
    }
    match MimeType::try_from(webrtc_codec.mime_type().as_str())? {
        MimeType::OPUS => Ok(Box::new(OpusSink::init(
            peer_id,
            output_device,
            track,
            webrtc_codec,
            sink_codec,
        )?)),
        _ => {
            bail!("unhandled mime type: {}", &webrtc_codec.mime_type());
        }
    }
}

pub fn get_input_device(
    host_id: cpal::HostId,
    device_name: String,
) -> anyhow::Result<cpal::Device> {
    let host = match cpal::available_hosts().iter().find(|h| **h == host_id) {
        Some(h) => match cpal::platform::host_from_id(*h) {
            Ok(h) => h,
            Err(e) => {
                bail!("failed to get host from id: {e}");
            }
        },
        None => {
            bail!("failed to find host");
        }
    };

    let device = host.input_devices()?.find(|d| {
        if let Ok(n) = d.name() {
            n == device_name
        } else {
            false
        }
    });

    if let Some(d) = device {
        Ok(d)
    } else {
        bail!("could not find device")
    }
}

pub fn get_output_device(
    host_id: cpal::HostId,
    device_name: String,
) -> anyhow::Result<cpal::Device> {
    let host = match cpal::available_hosts().iter().find(|h| **h == host_id) {
        Some(h) => match cpal::platform::host_from_id(*h) {
            Ok(h) => h,
            Err(e) => {
                bail!("failed to get host from id: {e}");
            }
        },
        None => {
            bail!("failed to find host");
        }
    };

    let device = host.output_devices()?.find(|d| {
        if let Ok(n) = d.name() {
            n == device_name
        } else {
            false
        }
    });

    if let Some(d) = device {
        Ok(d)
    } else {
        bail!("could not find device")
    }
}
