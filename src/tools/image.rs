use crate::env::FileError;

const PNG_SIGNATURE: [u8; 8] = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
const HEADER_BYTES: u64 = 32;
const BLOCK_BYTES: u64 = 64 * 1024;

pub type ReadAt<'a> = &'a mut dyn FnMut(u64, u64) -> Result<Vec<u8>, FileError>;

pub fn detect_supported_image_mime_type_of(
    size: u64,
    read: ReadAt<'_>,
) -> Result<Option<&'static str>, FileError> {
    let header = read(0, HEADER_BYTES)?;
    if !starts_with(&header, &PNG_SIGNATURE) {
        return Ok(detect_header(&header));
    }
    if is_png(&header) && !is_animated_png_of(size, read)? {
        return Ok(Some("image/png"));
    }
    Ok(None)
}

fn is_animated_png_of(size: u64, read: ReadAt<'_>) -> Result<bool, FileError> {
    let mut block: Vec<u8> = Vec::new();
    let mut block_start: u64 = 0;
    let mut offset: u64 = PNG_SIGNATURE.len() as u64;
    while offset + 8 <= size {
        if offset < block_start || offset + 8 > block_start + block.len() as u64 {
            block_start = offset;
            block = read(offset, BLOCK_BYTES)?;
        }
        let relative = (offset - block_start) as usize;
        let header = block.get(relative..).unwrap_or(&[]);
        let header = &header[..header.len().min(8)];
        if starts_with_ascii(header, 4, "acTL") {
            return Ok(true);
        }
        if starts_with_ascii(header, 4, "IDAT") {
            return Ok(false);
        }
        let next = offset + 8 + u64::from(read_u32_be(header, 0)) + 4;
        if next <= offset || next > size {
            return Ok(false);
        }
        offset = next;
    }
    Ok(false)
}

fn detect_header(buffer: &[u8]) -> Option<&'static str> {
    if starts_with(buffer, &[0xff, 0xd8, 0xff]) {
        return if buffer.get(3) == Some(&0xf7) {
            None
        } else {
            Some("image/jpeg")
        };
    }
    if starts_with_ascii(buffer, 0, "GIF87a") || starts_with_ascii(buffer, 0, "GIF89a") {
        return Some("image/gif");
    }
    if starts_with_ascii(buffer, 0, "RIFF") && starts_with_ascii(buffer, 8, "WEBP") {
        return Some("image/webp");
    }
    if starts_with_ascii(buffer, 0, "BM") && is_bmp(buffer) {
        return Some("image/bmp");
    }
    None
}

fn is_png(buffer: &[u8]) -> bool {
    buffer.len() >= 16 && read_u32_be(buffer, 8) == 13 && starts_with_ascii(buffer, 12, "IHDR")
}

fn is_bmp(buffer: &[u8]) -> bool {
    if buffer.len() < 26 {
        return false;
    }
    let declared_size = read_u32_le(buffer, 2);
    let pixel_offset = read_u32_le(buffer, 10);
    let dib_size = read_u32_le(buffer, 14);
    if declared_size != 0 && declared_size < 26 {
        return false;
    }
    if u64::from(pixel_offset) < 14 + u64::from(dib_size) {
        return false;
    }
    if declared_size != 0 && pixel_offset >= declared_size {
        return false;
    }
    let (planes, bits) = if dib_size == 12 {
        (read_u16_le(buffer, 22), read_u16_le(buffer, 24))
    } else if (40..=124).contains(&dib_size) {
        if buffer.len() < 30 {
            return false;
        }
        (read_u16_le(buffer, 26), read_u16_le(buffer, 28))
    } else {
        return false;
    };
    planes == 1 && [1, 4, 8, 16, 24, 32].contains(&bits)
}

fn byte(buffer: &[u8], offset: usize) -> u32 {
    u32::from(buffer.get(offset).copied().unwrap_or(0))
}

fn read_u16_le(buffer: &[u8], offset: usize) -> u32 {
    byte(buffer, offset) + (byte(buffer, offset + 1) << 8)
}

fn read_u32_be(buffer: &[u8], offset: usize) -> u32 {
    (byte(buffer, offset) << 24)
        | (byte(buffer, offset + 1) << 16)
        | (byte(buffer, offset + 2) << 8)
        | byte(buffer, offset + 3)
}

fn read_u32_le(buffer: &[u8], offset: usize) -> u32 {
    byte(buffer, offset)
        | (byte(buffer, offset + 1) << 8)
        | (byte(buffer, offset + 2) << 16)
        | (byte(buffer, offset + 3) << 24)
}

fn starts_with(buffer: &[u8], bytes: &[u8]) -> bool {
    buffer.starts_with(bytes)
}

fn starts_with_ascii(buffer: &[u8], offset: usize, text: &str) -> bool {
    buffer.get(offset..offset + text.len()) == Some(text.as_bytes())
}
