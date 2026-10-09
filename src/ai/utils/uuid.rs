use std::fs::File;
use std::io::{self, Read};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_UUID_V7_TIMESTAMP: u64 = 0xffff_ffff_ffff;
const MAX_SEQUENCE: u64 = (1 << 41) - 1;

struct Generator {
    last_ordinary_timestamp: u64,
    sequence: Option<u64>,
}

static GENERATOR: Mutex<Generator> = Mutex::new(Generator {
    last_ordinary_timestamp: 0,
    sequence: None,
});

fn random_bytes() -> io::Result<[u8; 16]> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

pub fn uuidv7(timestamp_ms: Option<u64>) -> Result<String, String> {
    let requested = timestamp_ms.unwrap_or_else(now_ms);
    if requested > MAX_UUID_V7_TIMESTAMP {
        return Err(format!(
            "UUIDv7 timestamp must be an integer between 0 and {MAX_UUID_V7_TIMESTAMP}"
        ));
    }
    let mut generator = GENERATOR
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let effective = match timestamp_ms {
        Some(explicit) => explicit,
        None => {
            let ordinary = requested.max(generator.last_ordinary_timestamp);
            generator.last_ordinary_timestamp = ordinary;
            ordinary
        }
    };
    let random = random_bytes().map_err(|error| error.to_string())?;
    let sequence = match generator.sequence {
        None => {
            (u64::from(random[1]) << 32)
                | (u64::from(random[2]) << 24)
                | (u64::from(random[3]) << 16)
                | (u64::from(random[4]) << 8)
                | u64::from(random[5])
        }
        Some(MAX_SEQUENCE) => return Err("UUIDv7 generator sequence exhausted".to_owned()),
        Some(previous) => previous + 1,
    };
    generator.sequence = Some(sequence);
    let mut bytes = random;
    for (index, byte) in bytes.iter_mut().take(6).enumerate() {
        *byte = (effective >> ((5 - index) * 8)) as u8;
    }
    bytes[6] = 0x70 | ((sequence >> 37) & 0x0f) as u8;
    bytes[7] = ((sequence >> 29) & 0xff) as u8;
    bytes[8] = 0x80 | ((sequence >> 23) & 0x3f) as u8;
    bytes[9] = ((sequence >> 15) & 0xff) as u8;
    bytes[10] = ((sequence >> 7) & 0xff) as u8;
    bytes[11] = (((sequence & 0x7f) << 1) as u8) | (random[11] & 0x01);
    let hex: Vec<String> = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        hex[0..4].concat(),
        hex[4..6].concat(),
        hex[6..8].concat(),
        hex[8..10].concat(),
        hex[10..16].concat()
    ))
}
