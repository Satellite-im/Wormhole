use std::{mem, slice, time::Duration};

use anyhow::bail;
use bytes::Bytes;
use clap::Parser;
use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait},
    SampleRate,
};
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

/// Test CPAL and OPUS
#[derive(Parser, Debug, Clone)]
enum Cli {
    /// Sets the sample type to be used by CPAL
    SampleType { sample_type: SampleTypes },
    /// the CPAL buffer size
    // BufferSize {buffer_size: u32},
    /// sets the number of bits per second in the opus encoder.
    /// accepted values range from 500 to 512000 bits per second
    BitRate { rate: i32 },
    /// sets the sampling frequency of the input signal.
    /// accepted values are 8000, 12000, 16000, 24000, and 48000
    SampleRate { rate: u32 },
    /// sets the number of samples per channel in the input signal.
    /// accepted values are 120, 240, 480, 960, 1920, and 2880
    FrameSize { frame_size: usize },
    /// sets the opus::Bandwidth. values are 1101 (4khz), 1102 (6kHz), 1103 (8kHz), 1104 (12kHz), and 1105 (20kHz)
    /// the kHz values represent the range of a bandpass filter
    Bandwidth { bandwidth: i32 },
    /// sets the opus::Application. values are 2048 (Voip) and 2049 (Audio)
    Application { application: i32 },
    /// records 10 seconds of audio
    Record,
    /// plays the most recently recorded audio
    Play,
    /// print the current config
    ShowConfig,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum SampleTypes {
    /// i16
    Signed,
    /// f32
    Float,
}

#[derive(Debug, Clone)]
struct StaticArgs {
    sample_type: SampleTypes,
    bit_rate: opus::Bitrate,
    sample_rate: u32,
    frame_size: usize,
    bandwidth: opus::Bandwidth,
    application: opus::Application,
}

struct EncodedSamples {
    data: Vec<u8>,
    idx: usize,
}
/// this will be used for audio processing. not going to put this in a mutex or send all the audio samples through a channel
/// because that's probably too slow.
static mut ENCODED_SAMPLES: EncodedSamples = EncodedSamples {
    data: vec![],
    idx: 0,
};

static STATIC_MEM: Lazy<Mutex<StaticArgs>> = Lazy::new(|| {
    Mutex::new(StaticArgs {
        sample_type: SampleTypes::Float,
        bit_rate: opus::Bitrate::Max,
        sample_rate: 8000,
        frame_size: 480,
        bandwidth: opus::Bandwidth::Narrowband,
        application: opus::Application::Voip,
    })
});

async fn handle_command(cli: Cli) -> anyhow::Result<()> {
    let mut sm = STATIC_MEM.lock().await;
    match cli {
        Cli::SampleType { sample_type } => {
            sm.sample_type = sample_type;
        }
        Cli::BitRate { rate } => {
            sm.bit_rate = opus::Bitrate::Bits(rate);
        }
        Cli::SampleRate { rate } => {
            if !vec![8000, 12000, 16000, 24000, 48000].contains(&rate) {
                bail!("invalid sample rate")
            }
            sm.sample_rate = rate;
        }
        Cli::FrameSize { frame_size } => {
            if !vec![120, 240, 480, 960, 1920, 2880].contains(&frame_size) {
                bail!("invalid frame size");
            }
            sm.frame_size = frame_size;
        }
        Cli::Bandwidth { bandwidth } => {
            sm.bandwidth = match bandwidth {
                -1000 => opus::Bandwidth::Auto,
                1101 => opus::Bandwidth::Narrowband,
                1102 => opus::Bandwidth::Mediumband,
                1103 => opus::Bandwidth::Wideband,
                1104 => opus::Bandwidth::Superwideband,
                1105 => opus::Bandwidth::Fullband,
                _ => bail!("invalid bandwidth"),
            };
        }
        Cli::Application { application } => {
            sm.application = match application {
                2048 => opus::Application::Voip,
                2049 => opus::Application::Audio,
                _ => bail!("invalid application"),
            };
        }
        Cli::Record => record_f32(sm.clone()).await?,
        Cli::Play => {}
        Cli::ShowConfig => println!("{:#?}", sm),
    }
    Ok(())
}

async fn record_f32(args: StaticArgs) -> anyhow::Result<()> {
    let duration_secs = 5;
    let total_samples = args.sample_rate as usize * 4 * (duration_secs + 1);
    unsafe {
        ENCODED_SAMPLES.data.resize(total_samples, 0);
        ENCODED_SAMPLES.idx = 0;
    }
    let config = cpal::StreamConfig {
        channels: 1,
        sample_rate: SampleRate(args.sample_rate),
        buffer_size: cpal::BufferSize::Default,
    };
    let mut packetizer =
        OpusPacketizer::init(args.frame_size, args.sample_rate, opus::Channels::Mono)?;

    // batch audio samples into a Packetizer, encode them via packetize(), and write the bytes to a global variable.
    let input_data_fn = move |data: &[f32], _: &cpal::InputCallbackInfo| unsafe {
        // if there isn't space for a new frame, then stop
        if ENCODED_SAMPLES.idx >= total_samples - args.frame_size {
            log::error!("ran out of space for samples");
            return;
        }
        let mut encoded: &mut [u8] =
            &mut ENCODED_SAMPLES.data.as_mut_slice()[ENCODED_SAMPLES.idx..];
        for sample in data {
            let r = match packetizer.packetize_f32(*sample, &mut encoded) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("failed to packetize: {e}");
                    continue;
                }
            };
            if let Some(size) = r {
                ENCODED_SAMPLES.idx += size;
            }
        }
    };
    let input_stream = cpal::default_host()
        .default_input_device()
        .ok_or(anyhow::anyhow!("no input device"))?
        .build_input_stream(&config.into(), input_data_fn, err_fn, None)
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to build input stream: {e}, {}, {}",
                file!(),
                line!()
            )
        })?;

    input_stream.play()?;
    tokio::time::sleep(Duration::from_secs(duration_secs as u64)).await;
    input_stream.pause()?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("starting REPL");
    println!("enter --help to see available commands");

    let mut iter = std::io::stdin().lines();
    while let Some(Ok(line)) = iter.next() {
        let mut v = vec![""];
        v.extend(line.split_ascii_whitespace());
        let cli = match Cli::try_parse_from(v) {
            Ok(r) => r,
            Err(e) => {
                println!("{e}");
                continue;
            }
        };
        if let Err(e) = handle_command(cli).await {
            println!("command failed: {e}");
        }
    }

    Ok(())
}

// opus::Encoder has separate functions for i16 and f32
// want to use the same struct for both functions. will do some unsafe stuff to accomplish this.
pub struct OpusPacketizer {
    // encodes groups of samples (frames)
    encoder: opus::Encoder,
    num_samples: usize,
    raw_bytes: Vec<u8>,
    // number of samples in a frame
    // todo: is this true? or is it the number of bytes...
    frame_size: usize,
}

impl OpusPacketizer {
    pub fn init(
        frame_size: usize,
        sample_rate: u32,
        channels: opus::Channels,
    ) -> anyhow::Result<Self> {
        let mut buf = Vec::new();
        buf.reserve(frame_size * 4);
        let encoder =
            opus::Encoder::new(sample_rate, channels, opus::Application::Voip).map_err(|e| {
                anyhow::anyhow!("{e}: sample_rate: {sample_rate}, channels: {channels:?}")
            })?;

        Ok(Self {
            encoder,
            num_samples: 0,
            raw_bytes: buf,
            frame_size,
        })
    }

    fn packetize_i16(&mut self, sample: i16, out: &mut [u8]) -> anyhow::Result<Option<usize>> {
        // opus::Encoder::encode is using raw pointers under the hood.
        let p: *const i16 = &sample;
        let bp: *const u8 = p as *const _;
        let bs: &[u8] = unsafe { slice::from_raw_parts(bp, mem::size_of::<i16>()) };
        self.raw_bytes.extend_from_slice(bs);
        self.num_samples += 1;
        if self.num_samples == self.frame_size {
            let p: *const i16 = self.raw_bytes.as_ptr() as _;
            let bs: &[i16] =
                unsafe { slice::from_raw_parts(p, mem::size_of::<i16>() * self.num_samples) };
            match self.encoder.encode(bs, out) {
                Ok(size) => {
                    self.raw_bytes.clear();
                    self.num_samples = 0;
                    return Ok(Some(size));
                }
                Err(e) => anyhow::bail!("failed to encode: {e}"),
            }
        } else {
            return Ok(None);
        }
    }

    fn packetize_f32(&mut self, sample: f32, out: &mut [u8]) -> anyhow::Result<Option<usize>> {
        // opus::Encoder::encode is using raw pointers under the hood.
        let p: *const f32 = &sample;
        let bp: *const u8 = p as *const _;
        let bs: &[u8] = unsafe { slice::from_raw_parts(bp, mem::size_of::<f32>()) };
        self.raw_bytes.extend_from_slice(bs);
        self.num_samples += 1;
        if self.num_samples == self.frame_size {
            let p: *const f32 = self.raw_bytes.as_ptr() as _;
            let bs: &[f32] =
                unsafe { slice::from_raw_parts(p, mem::size_of::<f32>() * self.num_samples) };
            match self.encoder.encode_float(bs, out) {
                Ok(size) => {
                    self.raw_bytes.clear();
                    self.num_samples = 0;
                    return Ok(Some(size));
                }
                Err(e) => anyhow::bail!("failed to encode: {e}"),
            }
        } else {
            return Ok(None);
        }
    }
}

fn err_fn(err: cpal::StreamError) {
    log::error!("an error occurred on stream: {}", err);
}
