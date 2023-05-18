use std::{fs::File, io::Write, mem, slice, time::Duration};

use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait},
    SampleRate,
};

use crate::{err_fn, packetizer::OpusPacketizer, StaticArgs, AUDIO_FILE_NAME};

// needs to be static for a callback
static mut AUDIO_FILE: Option<File> = None;

pub async fn record_f32_noencode(args: StaticArgs) -> anyhow::Result<()> {
    let duration_secs = args.audio_duration_secs;

    unsafe {
        AUDIO_FILE = Some(File::create(AUDIO_FILE_NAME)?);
    }
    let config = cpal::StreamConfig {
        channels: 1,
        sample_rate: SampleRate(args.sample_rate),
        buffer_size: cpal::BufferSize::Default,
    };

    // batch audio samples into a Packetizer, encode them via packetize(), and write the bytes to a global variable.
    let input_data_fn = move |data: &[f32], _: &cpal::InputCallbackInfo| {
        for sample in data {
            let arr = [*sample];
            let p: *const u8 = arr.as_ptr() as _;
            let bs: &[u8] = unsafe { slice::from_raw_parts(p, mem::size_of::<f32>() * 1) };
            unsafe {
                if let Some(mut f) = AUDIO_FILE.as_ref() {
                    if let Err(e) = f.write(bs) {
                        log::error!("failed to write bytes to file: {e}");
                    }
                }
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
    unsafe {
        if let Some(f) = AUDIO_FILE.as_ref() {
            f.sync_all()?;
        }
    }
    println!("finished recording audio");
    Ok(())
}

pub async fn record_f32_encode(args: StaticArgs) -> anyhow::Result<()> {
    unsafe {
        AUDIO_FILE = Some(File::create(AUDIO_FILE_NAME)?);
    }
    let config = cpal::StreamConfig {
        channels: 1,
        sample_rate: SampleRate(args.sample_rate),
        buffer_size: cpal::BufferSize::Default,
    };
    let mut packetizer =
        OpusPacketizer::init(args.frame_size, args.sample_rate, opus::Channels::Mono)?;

    let mut decoder = opus::Decoder::new(args.sample_rate, opus::Channels::Mono)?;

    // max frame size is 48kHz for 120ms
    const MAX_FRAME_SIZE: usize = 5760;
    // batch audio samples into a Packetizer, encode them via packetize(), and write the bytes to a global variable.
    let input_data_fn = move |data: &[f32], _: &cpal::InputCallbackInfo| {
        let mut encoded: [u8; MAX_FRAME_SIZE * 4] = [0; MAX_FRAME_SIZE * 4];
        let mut decoded: [f32; MAX_FRAME_SIZE] = [0_f32; MAX_FRAME_SIZE];
        for sample in data {
            let r: Option<usize> = match packetizer.packetize_f32(*sample, &mut encoded) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("failed to packetize: {e}");
                    continue;
                }
            };
            if let Some(size) = r {
                // decode_float returns the number of decoded samples
                match decoder.decode_float(&encoded[0..size], &mut decoded, false) {
                    Ok(decoded_samples) => unsafe {
                        if let Some(mut f) = AUDIO_FILE.as_ref() {
                            // cast the f32 array as a u8 array and save it.
                            let p: *const f32 = decoded.as_ptr();
                            let bp: *const u8 = p as _;
                            let bs: &[u8] =
                                slice::from_raw_parts(bp, mem::size_of::<f32>() * decoded_samples);
                            match f.write(bs) {
                                Ok(num_written) => {
                                    assert_eq!(num_written, mem::size_of::<f32>() * decoded_samples)
                                }
                                Err(e) => {
                                    log::error!("failed to write bytes to file: {e}");
                                }
                            }
                        }
                    },
                    Err(e) => {
                        log::error!("failed to decode float: {e}");
                        return;
                    }
                }
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
    tokio::time::sleep(Duration::from_secs(args.audio_duration_secs as u64)).await;
    input_stream.pause()?;
    unsafe {
        if let Some(f) = AUDIO_FILE.as_ref() {
            f.sync_all()?;
        }
    }
    println!("finished recording audio");
    Ok(())
}
