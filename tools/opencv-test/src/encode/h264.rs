use super::Args;
use anyhow::Result;

use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
};

use opencv::{prelude::*, videoio};

pub fn encode_h264(args: Args) -> Result<()> {
    let cam = videoio::VideoCapture::from_file(&args.input, videoio::CAP_ANY)?;
    let opened = videoio::VideoCapture::is_opened(&cam)?;
    if !opened {
        panic!("Unable to open video file!");
    }

    // https://docs.opencv.org/3.4/d4/d15/group__videoio__flags__base.html
    let frame_width = cam.get(3)? as u32;
    let frame_height = cam.get(4)? as u32;
    let fps = cam.get(5)? as _;

    let output_file = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(args.output)?;
    let mut writer = BufWriter::new(output_file);

    let config = openh264::encoder::EncoderConfig::new(frame_width * 2, frame_height * 2)
        .max_frame_rate(fps); //.rate_control_mode(openh264::encoder::RateControlMode::Timestamp);

    let mut encoder = openh264::encoder::Encoder::with_config(config)?;

    let mut iter = crate::VideoFileIter::new(cam);
    while let Some(mut frame) = iter.next() {
        let sz = frame.size()?;
        let width = sz.width as usize;
        let height = sz.height as usize;
        if width == 0 {
            continue;
        }
        let p = frame.data_mut();
        let len = width * height * 3;
        let s = std::ptr::slice_from_raw_parts(p, len as _);
        let s: &[u8] = unsafe { &*s };

        let yuv = crate::utils::bgr_to_yuv420_full_scale(s, width, height);

        let yuv_buf = crate::utils::YUVBuf {
            yuv,
            width: width * 2,
            height: height * 2,
        };

        let encoded_stream = encoder.encode(&yuv_buf)?;
        encoded_stream.write(&mut writer)?;
    }
    writer.flush()?;
    Ok(())
}
