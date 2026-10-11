use std::time::{SystemTime, UNIX_EPOCH};

use ring::{digest, hmac};

pub struct Signing<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub region: &'a str,
    pub service: &'a str,
}

pub fn amz_date(now: SystemTime) -> String {
    let seconds = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs()) as i64;
    let days = seconds.div_euclid(86_400);
    let within = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        within / 3600,
        within % 3600 / 60,
        within % 60
    )
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

pub fn uri_encode(text: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(byte));
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

pub fn authorization(
    signing: &Signing,
    amz_date: &str,
    method: &str,
    canonical_path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> String {
    let mut signed: Vec<(String, String)> = headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    signed.sort();
    let names = signed
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = signed
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let canonical = format!(
        "{method}\n{canonical_path}\n\n{canonical_headers}\n{names}\n{}",
        hex(digest::digest(&digest::SHA256, body).as_ref())
    );
    let date = &amz_date[..8];
    let scope = format!("{date}/{}/{}/aws4_request", signing.region, signing.service);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(digest::digest(&digest::SHA256, canonical.as_bytes()).as_ref())
    );
    let key_date = mac(
        format!("AWS4{}", signing.secret_key).as_bytes(),
        date.as_bytes(),
    );
    let key_region = mac(&key_date, signing.region.as_bytes());
    let key_service = mac(&key_region, signing.service.as_bytes());
    let key_signing = mac(&key_service, b"aws4_request");
    let signature = hex(&mac(&key_signing, to_sign.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={signature}",
        signing.access_key
    )
}

fn mac(key: &[u8], data: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
        .as_ref()
        .to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
