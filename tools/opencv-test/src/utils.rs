// shamelessly stolen from here: https://github.com/hanguk0726/Avatar-Vision/blob/main/rust/src/tools/image_processing.rs

use openh264::formats::YUVSource;

pub fn bgr_to_rgba(data: &[u8]) -> Vec<u8> {
    let mut rgba = Vec::with_capacity(data.len() * 2);
    for chunk in data.chunks_exact(3) {
        rgba.extend_from_slice(&[chunk[2], chunk[1], chunk[0], 255]);
    }
    rgba
}

pub fn yuyv422_to_rgb_(data: &[u8], rgba: bool) -> Vec<u8> {
    let mut rgb = Vec::with_capacity(data.len() * 2);
    for chunk in data.chunks_exact(4) {
        let y0 = chunk[0] as f32;
        let u = chunk[1] as f32;
        let y1 = chunk[2] as f32;
        let v: f32 = chunk[3] as f32;

        let r0 = y0 + 1.370705 * (v - 128.);
        let g0 = y0 - 0.698001 * (v - 128.) - 0.337633 * (u - 128.);
        let b0 = y0 + 1.732446 * (u - 128.);

        let r1 = y1 + 1.370705 * (v - 128.);
        let g1 = y1 - 0.698001 * (v - 128.) - 0.337633 * (u - 128.);
        let b1 = y1 + 1.732446 * (u - 128.);

        if rgba {
            rgb.extend_from_slice(&[
                r0 as u8, g0 as u8, b0 as u8, 255, r1 as u8, g1 as u8, b1 as u8, 255,
            ]);
        } else {
            rgb.extend_from_slice(&[r0 as u8, g0 as u8, b0 as u8, r1 as u8, g1 as u8, b1 as u8]);
        }
    }
    rgb
}

pub fn bgr_to_yuv(rgba: &[u8], width: usize, height: usize) -> Vec<u8> {
    let size = (3 * width * height) / 2;
    let mut yuv = vec![0; size];

    let u_base = width * height;
    let v_base = u_base + u_base / 4;
    let half_width = width / 2;

    // y is full size, u, v is quarter size
    let pixel = |x: usize, y: usize| -> (f32, f32, f32) {
        // two dim to single dim
        let base_pos = (x + y * width) * 3;
        (
            rgba[base_pos + 2] as f32,
            rgba[base_pos + 1] as f32,
            rgba[base_pos + 0] as f32,
        )
    };

    let write_y = |yuv: &mut [u8], x: usize, y: usize, rgb: (f32, f32, f32)| {
        yuv[x + y * width] =
            (0.2578125 * rgb.0 + 0.50390625 * rgb.1 + 0.09765625 * rgb.2 + 16.0) as u8;
    };

    let write_u = |yuv: &mut [u8], x: usize, y: usize, rgb: (f32, f32, f32)| {
        yuv[u_base + x + y * half_width] =
            (-0.1484375 * rgb.0 + -0.2890625 * rgb.1 + 0.4375 * rgb.2 + 128.0) as u8;
    };

    let write_v = |yuv: &mut [u8], x: usize, y: usize, rgb: (f32, f32, f32)| {
        yuv[v_base + x + y * half_width] =
            (0.4375 * rgb.0 + -0.3671875 * rgb.1 + -0.0703125 * rgb.2 + 128.0) as u8;
    };
    for i in 0..width / 2 {
        for j in 0..height / 2 {
            let px = i * 2;
            let py = j * 2;
            let pix0x0 = pixel(px, py);
            let pix0x1 = pixel(px, py + 1);
            let pix1x0 = pixel(px + 1, py);
            let pix1x1 = pixel(px + 1, py + 1);
            let avg_pix = (
                (pix0x0.0 as u32 + pix0x1.0 as u32 + pix1x0.0 as u32 + pix1x1.0 as u32) as f32
                    / 4.0,
                (pix0x0.1 as u32 + pix0x1.1 as u32 + pix1x0.1 as u32 + pix1x1.1 as u32) as f32
                    / 4.0,
                (pix0x0.2 as u32 + pix0x1.2 as u32 + pix1x0.2 as u32 + pix1x1.2 as u32) as f32
                    / 4.0,
            );
            write_y(&mut yuv[..], px, py, pix0x0);
            write_y(&mut yuv[..], px, py + 1, pix0x1);
            write_y(&mut yuv[..], px + 1, py, pix1x0);
            write_y(&mut yuv[..], px + 1, py + 1, pix1x1);
            write_u(&mut yuv[..], i, j, avg_pix);
            write_v(&mut yuv[..], i, j, avg_pix);
        }
    }
    yuv
}

// doesn't use full scale coeffecients
pub fn bgr_to_yuv_limited(rgba: &[u8], width: usize, height: usize) -> Vec<u8> {
    let size = (3 * width * height) / 2;
    let mut yuv = vec![0; size];

    let u_base = width * height;
    let v_base = u_base + u_base / 4;
    let half_width = width / 2;

    // y is full size, u, v is quarter size
    let pixel = |x: usize, y: usize| -> (f32, f32, f32) {
        // two dim to single dim
        let base_pos = (x + y * width) * 3;
        (
            rgba[base_pos + 2] as f32,
            rgba[base_pos + 1] as f32,
            rgba[base_pos + 0] as f32,
        )
    };

    let write_y = |yuv: &mut [u8], x: usize, y: usize, rgb: (f32, f32, f32)| {
        yuv[x + y * width] =
            (0.2578125 * rgb.0 + 0.50390625 * rgb.1 + 0.09765625 * rgb.2 + 16.0) as u8;
    };

    let write_u = |yuv: &mut [u8], x: usize, y: usize, rgb: (f32, f32, f32)| {
        yuv[u_base + x + y * half_width] =
            (-0.1484375 * rgb.0 + -0.2890625 * rgb.1 + 0.4375 * rgb.2 + 128.0) as u8;
    };

    let write_v = |yuv: &mut [u8], x: usize, y: usize, rgb: (f32, f32, f32)| {
        yuv[v_base + x + y * half_width] =
            (0.4375 * rgb.0 + -0.3671875 * rgb.1 + -0.0703125 * rgb.2 + 128.0) as u8;
    };
    for i in 0..width / 2 {
        for j in 0..height / 2 {
            let px = i * 2;
            let py = j * 2;
            let pix0x0 = pixel(px, py);
            let pix0x1 = pixel(px, py + 1);
            let pix1x0 = pixel(px + 1, py);
            let pix1x1 = pixel(px + 1, py + 1);
            let avg_pix = (
                (pix0x0.0 as u32 + pix0x1.0 as u32 + pix1x0.0 as u32 + pix1x1.0 as u32) as f32
                    / 4.0,
                (pix0x0.1 as u32 + pix0x1.1 as u32 + pix1x0.1 as u32 + pix1x1.1 as u32) as f32
                    / 4.0,
                (pix0x0.2 as u32 + pix0x1.2 as u32 + pix1x0.2 as u32 + pix1x1.2 as u32) as f32
                    / 4.0,
            );
            write_y(&mut yuv[..], px, py, pix0x0);
            write_y(&mut yuv[..], px, py + 1, pix0x1);
            write_y(&mut yuv[..], px + 1, py, pix1x0);
            write_y(&mut yuv[..], px + 1, py + 1, pix1x1);
            write_u(&mut yuv[..], i, j, avg_pix);
            write_v(&mut yuv[..], i, j, avg_pix);
        }
    }
    yuv
}

pub struct YUVBuf {
    pub yuv: Vec<u8>,
    pub width: usize,
    pub height: usize,
}

impl av_data::frame::FrameBuffer for YUVBuf {
    fn linesize(&self, idx: usize) -> Result<usize, av_data::frame::FrameError> {
        match idx {
            0 => Ok(self.width),
            1 | 2 => Ok(self.width >> 2),
            _ => Err(av_data::frame::FrameError::InvalidIndex),
        }
    }

    fn count(&self) -> usize {
        3
    }

    fn as_slice_inner(&self, idx: usize) -> Result<&[u8], av_data::frame::FrameError> {
        let base_u = self.width * self.height;
        let base_v = base_u + (base_u / 4);
        match idx {
            0 => Ok(&self.yuv[0..self.width * self.height]),
            1 => Ok(&self.yuv[base_u..base_v]),
            2 => Ok(&self.yuv[base_v..]),
            _ => Err(av_data::frame::FrameError::InvalidIndex),
        }
    }

    fn as_mut_slice_inner(&mut self, idx: usize) -> Result<&mut [u8], av_data::frame::FrameError> {
        let base_u = self.width * self.height;
        let base_v = base_u + (base_u / 4);
        match idx {
            0 => Ok(&mut self.yuv[0..self.width * self.height]),
            1 => Ok(&mut self.yuv[base_u..base_v]),
            2 => Ok(&mut self.yuv[base_v..]),
            _ => Err(av_data::frame::FrameError::InvalidIndex),
        }
    }
}

impl YUVSource for YUVBuf {
    fn width(&self) -> i32 {
        self.width as i32
    }

    fn height(&self) -> i32 {
        self.height as i32
    }

    fn y(&self) -> &[u8] {
        &self.yuv[0..self.width * self.height]
    }

    fn u(&self) -> &[u8] {
        let base = self.width * self.height;
        &self.yuv[base..base + base / 4]
    }

    fn v(&self) -> &[u8] {
        let base_u = self.width * self.height;
        let base_v = base_u + (base_u / 4);
        &self.yuv[base_v..]
    }

    fn y_stride(&self) -> i32 {
        self.width as _
    }

    fn u_stride(&self) -> i32 {
        self.width as i32 / 2
    }

    fn v_stride(&self) -> i32 {
        self.width as i32 / 2
    }
}

// attempts to avoid the loss when converting from BGR to YUV420 by quadrupling the size of the output. this ensures no UV samples are discarded/averaged
// unfortunately is still lossy
pub fn bgr_to_yuv_lossy(s: &[u8], width: usize, height: usize) -> Vec<u8> {
    // for y
    let y_rc_2_idx = |row: usize, col: usize| (row * width * 2) + col;

    let get_y = |rgb: (f32, f32, f32)| {
        // best. appears to be from the wikipedia page on YCbCr
        (0.2578125 * rgb.0 + 0.50390625 * rgb.1 + 0.09765625 * rgb.2 + 16.0) as u8
        // full scale:   // https://web.archive.org/web/20180423091842/http://www.equasys.de/colorconversion.html
        //(0.299 * rgb.0 + 0.587 * rgb.1 + 0.114 * rgb.2 + 0.0) as u8
        // hdtv
        //(0.183 * rgb.0 + 0.614 * rgb.1 + 0.062 * rgb.2 + 16.0) as u8
    };

    let get_u = |rgb: (f32, f32, f32)| {
        (-0.1484375 * rgb.0 + -0.2890625 * rgb.1 + 0.4375 * rgb.2 + 128.0) as u8
        //(-0.169 * rgb.0 + -0.331 * rgb.1 + 0.500 * rgb.2 + 128.0) as u8
        //(-0.101 * rgb.0 + -0.339 * rgb.1 + 0.439 * rgb.2 + 128.0) as u8
    };

    let get_v = |rgb: (f32, f32, f32)| {
        (0.4375 * rgb.0 + -0.3671875 * rgb.1 + -0.0703125 * rgb.2 + 128.0) as u8
        //(0.500 * rgb.0 + -0.419 * rgb.1 + -0.081 * rgb.2 + 128.0) as u8
        //(0.439 * rgb.0 + -0.399 * rgb.1 + -0.040 * rgb.2 + 128.0) as u8
    };

    let yuv_len = (width * height) * 6;
    let mut yuv: Vec<u8> = Vec::new();
    yuv.resize(yuv_len, 0);
    let u_base = (width * height) * 4;
    let v_base = u_base + (width * height);
    let mut uv_idx = 0;
    for row in 0..height {
        for col in 0..width {
            let base_pos = (col + row * width) * 3;
            let b = s[base_pos];
            let g = s[base_pos + 1];
            let r = s[base_pos + 2];

            let rgb = (r as _, g as _, b as _);
            let (y, u, v) = (get_y(rgb), get_u(rgb), get_v(rgb));

            // each byte in the u/v plane corresponds to a 4x4 square on the y plane
            let y_row = row * 2;
            let y_col = col * 2;

            let idx = y_rc_2_idx(y_row, y_col);
            yuv[idx] = y;
            yuv[idx + 1] = y;
            let idx = y_rc_2_idx(y_row + 1, y_col);
            yuv[idx] = y;
            yuv[idx + 1] = y;

            yuv[u_base + uv_idx] = u;
            yuv[v_base + uv_idx] = v;
            uv_idx += 1;
        }
    }

    yuv
}

pub struct RGBBuf {
    pub data: Vec<u8>,
    pub width: usize,
    pub height: usize,
}

impl RGBBuf {
    pub fn from_gbr(gbr: &[u8], width: usize, height: usize) -> Self {
        let mut data = Vec::new();
        data.extend_from_slice(gbr);

        for chunk in data.chunks_exact_mut(3) {
            let x = chunk[0];
            chunk[0] = chunk[2];
            chunk[2] = x;
        }

        Self {
            data,
            width,
            height,
        }
    }
}

impl av_data::frame::FrameBuffer for RGBBuf {
    fn linesize(&self, idx: usize) -> Result<usize, av_data::frame::FrameError> {
        match idx {
            0..=2 => Ok(self.width),
            _ => Err(av_data::frame::FrameError::InvalidIndex),
        }
    }

    fn count(&self) -> usize {
        3
    }

    fn as_slice_inner(&self, idx: usize) -> Result<&[u8], av_data::frame::FrameError> {
        match idx {
            0..=2 => Ok(&self.data[0..]),
            _ => Err(av_data::frame::FrameError::InvalidIndex),
        }
    }

    fn as_mut_slice_inner(&mut self, idx: usize) -> Result<&mut [u8], av_data::frame::FrameError> {
        match idx {
            0..=2 => Ok(&mut self.data[0..]),
            _ => Err(av_data::frame::FrameError::InvalidIndex),
        }
    }
}