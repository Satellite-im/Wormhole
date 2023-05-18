use anyhow::bail;
use clap::Parser;

use encode::*;
use log::LevelFilter;
use once_cell::sync::Lazy;
use play::*;
use record::*;
use simple_logger::SimpleLogger;
use tokio::sync::Mutex;

mod encode;
mod feedback;
mod packetizer;
mod play;
mod record;

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
    /// records 10 seconds of audio and writes it to a file
    Record { file_name: String },
    /// encode and decode the given file, saving the output to a new file
    Encode {
        input_file_name: String,
        output_file_name: String,
    },
    /// tests encoding/decoding with specified decoding parameters
    CustomEncode {
        decoded_sample_rate: u32,
        input_file_name: String,
        output_file_name: String,
    },
    /// plays the given file
    Play { file_name: String },
    /// print the current config
    ShowConfig,
    /// test feeding the input and output streams together
    Feedback,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum SampleTypes {
    /// i16
    Signed,
    /// f32
    Float,
}

#[derive(Debug, Clone)]
pub struct StaticArgs {
    sample_type: SampleTypes,
    bit_rate: opus::Bitrate,
    sample_rate: u32,
    frame_size: usize,
    bandwidth: opus::Bandwidth,
    application: opus::Application,
    audio_duration_secs: usize,
}

// CPAL callbacks have a static lifetime. in play.rs and record.rs, a global variable is used to share data between callbacks.
// that variable is a file, named by AUDIO_FILE_NAME.
static mut AUDIO_FILE_NAME: Lazy<String> = Lazy::new(|| String::from("/tmp/audio.bin"));
static STATIC_MEM: Lazy<Mutex<StaticArgs>> = Lazy::new(|| {
    Mutex::new(StaticArgs {
        sample_type: SampleTypes::Float,
        bit_rate: opus::Bitrate::Max,
        sample_rate: 8000,
        frame_size: 240,
        bandwidth: opus::Bandwidth::Fullband,
        application: opus::Application::Voip,
        audio_duration_secs: 5,
    })
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        .init()
        .unwrap();

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
        // based on OPUS RFC, OPUS encodes frames based on duration - 2.5, 5, 10, 20, 40, or 60ms.
        // This means that for a given sample rate, not all frame sizes are acceptable.
        // sample rate of 8000 with a frame size of:  480 - 60ms; 240 - 30ms
        // sample rate of 16000 with a frame size of: 960 - 60ms
        // sample rate of 24000 with a frame size of: 480 - 20ms; 240 - 10ms
        // sample rate of 48000 with a frame size of: 120: 2.5ms; 240: 5ms; 480: 10ms; 960: 10ms; 1920: 20ms;
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
        Cli::Record { file_name } => {
            unsafe {
                *AUDIO_FILE_NAME = file_name;
            }
            match sm.sample_type {
                SampleTypes::Float => record_f32(sm.clone()).await?,
                SampleTypes::Signed => todo!(),
            }
        }
        Cli::Encode {
            input_file_name,
            output_file_name,
        } => {
            // todo
            match sm.sample_type {
                SampleTypes::Float => {
                    encode_f32(
                        sm.clone(),
                        sm.sample_rate,
                        input_file_name,
                        output_file_name,
                    )
                    .await?
                }
                SampleTypes::Signed => todo!(),
            }
        }
        Cli::CustomEncode {
            decoded_sample_rate,
            input_file_name,
            output_file_name,
        } => {
            encode_f32(
                sm.clone(),
                decoded_sample_rate,
                input_file_name,
                output_file_name,
            )
            .await?;
        }
        Cli::Play { file_name } => {
            unsafe {
                *AUDIO_FILE_NAME = file_name;
            }
            match sm.sample_type {
                SampleTypes::Float => play_f32(sm.clone()).await?,
                SampleTypes::Signed => todo!(),
            }
        }
        Cli::ShowConfig => println!("{:#?}", sm),
        Cli::Feedback => feedback::feedback(sm.clone()).await?,
    }
    Ok(())
}

pub fn err_fn(err: cpal::StreamError) {
    log::error!("an error occurred on stream: {}", err);
}