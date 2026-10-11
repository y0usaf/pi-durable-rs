use std::collections::BTreeMap;
use std::io::{self, Read};

const PRELUDE_LEN: usize = 12;
const CHECKSUM_LEN: usize = 4;
const MIN_MESSAGE_LEN: usize = PRELUDE_LEN + CHECKSUM_LEN;
const MAX_MESSAGE_LEN: usize = 16 * 1024 * 1024;
const TRUNCATED: &str = "Truncated event message received.";
const OUT_OF_BOUNDS: &str = "Offset is outside the bounds of the DataView";

const CRC_TABLE: [u32; 256] = crc_table();

const fn crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut index = 0;
    while index < 256 {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 != 0 {
                0xEDB8_8320 ^ (value >> 1)
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

fn crc32_update(crc: u32, data: &[u8]) -> u32 {
    let mut value = !crc;
    for &byte in data {
        value = CRC_TABLE[((value ^ u32::from(byte)) & 0xff) as usize] ^ (value >> 8);
    }
    !value
}

fn fill(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..])? {
            0 => break,
            count => filled += count,
        }
    }
    Ok(filled)
}

pub fn read_frame(reader: &mut impl Read) -> Result<Option<Frame>, String> {
    let mut prelude = [0u8; PRELUDE_LEN];
    let got = fill(reader, &mut prelude).map_err(|error| error.to_string())?;
    if got == 0 {
        return Ok(None);
    }
    if got < PRELUDE_LEN {
        return Err(TRUNCATED.to_owned());
    }
    let total = u32::from_be_bytes([prelude[0], prelude[1], prelude[2], prelude[3]]) as usize;
    let headers_len = u32::from_be_bytes([prelude[4], prelude[5], prelude[6], prelude[7]]) as usize;
    let expected_prelude = u32::from_be_bytes([prelude[8], prelude[9], prelude[10], prelude[11]]);
    let prelude_crc = crc32_update(0, &prelude[..8]);
    if prelude_crc != expected_prelude {
        return Err(format!(
            "The prelude checksum specified in the message ({expected_prelude}) does not match the calculated CRC32 checksum ({prelude_crc})"
        ));
    }
    if !(MIN_MESSAGE_LEN..=MAX_MESSAGE_LEN).contains(&total) {
        return Err(OUT_OF_BOUNDS.to_owned());
    }
    if headers_len > total - MIN_MESSAGE_LEN {
        return Err(OUT_OF_BOUNDS.to_owned());
    }
    let mut rest = vec![0u8; total - PRELUDE_LEN];
    reader
        .read_exact(&mut rest)
        .map_err(|_| TRUNCATED.to_owned())?;
    let body_end = rest.len() - CHECKSUM_LEN;
    let expected_message = u32::from_be_bytes([
        rest[body_end],
        rest[body_end + 1],
        rest[body_end + 2],
        rest[body_end + 3],
    ]);
    let message_crc = crc32_update(crc32_update(prelude_crc, &prelude[8..]), &rest[..body_end]);
    if message_crc != expected_message {
        return Err(format!(
            "The message checksum ({message_crc}) did not match the expected value of {expected_message}"
        ));
    }
    let headers = parse_headers(&rest[..headers_len])?;
    let payload = rest[headers_len..body_end].to_vec();
    Ok(Some(Frame { headers, payload }))
}

pub struct Frame {
    headers: BTreeMap<String, String>,
    payload: Vec<u8>,
}

impl Frame {
    pub fn into_event(self) -> Result<(String, Vec<u8>), String> {
        let header = |name: &str| self.headers.get(name).cloned().unwrap_or_default();
        match header(":message-type").as_str() {
            "event" => Ok((header(":event-type"), self.payload)),
            "error" => {
                let message = header(":error-message");
                let message = if message.is_empty() {
                    "UnknownError".to_owned()
                } else {
                    message
                };
                Err(format!("{}: {message}", header(":error-code")))
            }
            "exception" => {
                let code = header(":exception-type");
                let message = serde_json::from_slice::<serde_json::Value>(&self.payload)
                    .ok()
                    .and_then(|body| body["message"].as_str().map(str::to_owned))
                    .unwrap_or_else(|| String::from_utf8_lossy(&self.payload).into_owned());
                Err(format!("{code}: {message}"))
            }
            other => Err(format!("Unrecognizable event type: {other}")),
        }
    }
}

fn take<'a>(bytes: &'a [u8], position: &mut usize, count: usize) -> Result<&'a [u8], String> {
    let end = position
        .checked_add(count)
        .filter(|end| *end <= bytes.len())
        .ok_or(OUT_OF_BOUNDS)?;
    let slice = &bytes[*position..end];
    *position = end;
    Ok(slice)
}

fn parse_headers(bytes: &[u8]) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    let mut position = 0;
    while position < bytes.len() {
        let name_len = take(bytes, &mut position, 1)?[0] as usize;
        let name = String::from_utf8_lossy(take(bytes, &mut position, name_len)?).into_owned();
        let tag = take(bytes, &mut position, 1)?[0];
        match tag {
            0 | 1 => {}
            2 => {
                take(bytes, &mut position, 1)?;
            }
            3 => {
                take(bytes, &mut position, 2)?;
            }
            4 => {
                take(bytes, &mut position, 4)?;
            }
            5 | 8 => {
                take(bytes, &mut position, 8)?;
            }
            6 | 7 => {
                let length = take(bytes, &mut position, 2)?;
                let length = u16::from_be_bytes([length[0], length[1]]) as usize;
                let value = take(bytes, &mut position, length)?;
                if tag == 7 {
                    out.insert(name, String::from_utf8_lossy(value).into_owned());
                }
            }
            9 => {
                take(bytes, &mut position, 16)?;
            }
            _ => return Err("Unrecognized header type tag".to_owned()),
        }
    }
    Ok(out)
}
