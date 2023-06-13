use std::cmp::Ordering;

use bytes::Bytes;
use opus::Bitrate;
use warp::blink;
use webrtc_audio_processing::{EchoCancellation, EchoCancellationSuppressionLevel};

use crate::host_media::audio::{
    loudness,
    opus::{ChannelMixer, ChannelMixerConfig, ChannelMixerOutput, Resampler, ResamplerConfig},
};

pub struct Framer {
    // encodes groups of samples (frames)
    encoder: opus::Encoder,
    // queues samples, to build a frame
    // options are i16 and f32. seems safer to use a f32.
    raw_samples: Vec<f32>,
    // used for the encoder
    opus_out: Vec<u8>,
    // number of samples in a frame
    frame_size: usize,
    // for splitting and merging audio channels
    channel_mixer: ChannelMixer,
    // for upsampling and downsampling audio
    resampler: Resampler,
    loudness_calculator: loudness::Calculator,
    audio_processor: Option<webrtc_audio_processing::Processor>,
    echo_cancellation_strategy: Option<blink::EchoCancellationStrategy>,
}

pub struct FramerOutput {
    pub bytes: Bytes,
    pub loudness: f32,
}

impl Framer {
    pub fn init(
        frame_size: usize,
        webrtc_codec: blink::AudioCodec,
        source_codec: blink::AudioCodec,
        echo_cancellation_config: Option<blink::EchoCancellationConfig>,
    ) -> anyhow::Result<Self> {
        let frame_size = frame_size * webrtc_codec.channels() as usize;
        let loudness_calculator = loudness::Calculator::new(frame_size);

        let echo_cancellation_strategy = echo_cancellation_config
            .as_ref()
            .map(|config| config.strategy.clone());
        let audio_processor = match &echo_cancellation_config {
            Some(config) => {
                let mut processor = webrtc_audio_processing::Processor::new(
                    &webrtc_audio_processing::InitializationConfig {
                        num_capture_channels: webrtc_codec.channels() as i32,
                        num_render_channels: webrtc_codec.channels() as i32,
                        ..Default::default()
                    },
                )?;

                let suppression_level = match config.intensity {
                    blink::EchoCancellationIntensity::Low => EchoCancellationSuppressionLevel::Low,
                    blink::EchoCancellationIntensity::Medium => {
                        EchoCancellationSuppressionLevel::Moderate
                    }
                    blink::EchoCancellationIntensity::High => {
                        EchoCancellationSuppressionLevel::High
                    }
                };

                processor.set_config(webrtc_audio_processing::Config {
                    echo_cancellation: Some(EchoCancellation {
                        suppression_level,
                        stream_delay_ms: None,
                        enable_delay_agnostic: true,
                        enable_extended_filter: true,
                    }),
                    enable_high_pass_filter: true,
                    ..Default::default()
                });

                Some(processor)
            }
            None => None,
        };

        let mut buf: Vec<f32> = Vec::new();
        buf.reserve(frame_size);
        let mut opus_out: Vec<u8> = Vec::new();
        opus_out.resize(frame_size * 4, 0);
        let mut encoder = opus::Encoder::new(
            webrtc_codec.sample_rate(),
            opus::Channels::Mono,
            opus::Application::Voip,
        )
        .map_err(|e| anyhow::anyhow!("{e}: sample_rate: {}", webrtc_codec.sample_rate()))?;
        // todo: abstract this
        encoder.set_bitrate(Bitrate::Bits(16000))?;

        let resampler_config = match webrtc_codec.sample_rate().cmp(&source_codec.sample_rate()) {
            Ordering::Equal => ResamplerConfig::None,
            Ordering::Greater => {
                ResamplerConfig::UpSample(webrtc_codec.sample_rate() / source_codec.sample_rate())
            }
            _ => {
                ResamplerConfig::DownSample(source_codec.sample_rate() / webrtc_codec.sample_rate())
            }
        };

        let channel_mixer_config = match webrtc_codec.channels().cmp(&source_codec.channels()) {
            Ordering::Equal => ChannelMixerConfig::None,
            Ordering::Less => ChannelMixerConfig::Merge,
            _ => ChannelMixerConfig::Split,
        };

        Ok(Self {
            encoder,
            raw_samples: buf,
            opus_out,
            frame_size,
            resampler: Resampler::new(resampler_config),
            channel_mixer: ChannelMixer::new(channel_mixer_config),
            loudness_calculator,
            audio_processor,
            echo_cancellation_strategy,
        })
    }

    pub fn frame(&mut self, sample: f32) -> Option<FramerOutput> {
        match self.channel_mixer.process(sample) {
            ChannelMixerOutput::Single(sample) => {
                self.resampler.process(sample, &mut self.raw_samples);
            }
            ChannelMixerOutput::Split(sample) => {
                self.resampler.process(sample, &mut self.raw_samples);
                self.resampler.process(sample, &mut self.raw_samples);
            }
            ChannelMixerOutput::None => {}
        }

        // frame_size should be 480 * num_channels
        if self.raw_samples.len() == self.frame_size {
            if let Some(processor) = self.audio_processor.as_mut() {
                let strategy = self
                    .echo_cancellation_strategy
                    .as_ref()
                    .unwrap_or(&blink::EchoCancellationStrategy::Normal);

                if matches!(
                    strategy,
                    blink::EchoCancellationStrategy::Normal
                        | blink::EchoCancellationStrategy::DoubleMax
                        | blink::EchoCancellationStrategy::DoubleInput
                ) {
                    if let Err(e) = processor.process_capture_frame(self.raw_samples.as_mut_slice())
                    {
                        log::error!("failed to process capture frame: {e}");
                    }
                }

                if matches!(
                    strategy,
                    blink::EchoCancellationStrategy::DoubleMax
                        | blink::EchoCancellationStrategy::DoubleInput
                ) {
                    if let Err(e) = processor.process_render_frame(self.raw_samples.as_mut_slice())
                    {
                        log::error!("failed to process render frame: {e}");
                    }
                }
            }

            for sample in self.raw_samples.iter() {
                self.loudness_calculator.insert(*sample);
            }

            match self.encoder.encode_float(
                self.raw_samples.as_mut_slice(),
                self.opus_out.as_mut_slice(),
            ) {
                Ok(size) => {
                    self.raw_samples.clear();
                    let slice = self.opus_out.as_slice();
                    let bytes = bytes::Bytes::copy_from_slice(&slice[0..size]);

                    let loudness = self.loudness_calculator.get_rms();
                    Some(FramerOutput { bytes, loudness })
                }
                Err(e) => {
                    self.raw_samples.clear();
                    log::error!("OpusPacketizer failed to encode: {}", e);
                    None
                }
            }
        } else {
            None
        }
    }
}
