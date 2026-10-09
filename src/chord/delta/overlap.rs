pub fn overlap(a: &str, b: &str, scan: usize, probe: usize, max_candidates: usize) -> usize {
    let a: Vec<u16> = a.encode_utf16().collect();
    let b: Vec<u16> = b.encode_utf16().collect();
    if a.is_empty() || b.is_empty() || scan == 0 {
        return 0;
    }
    let tail = if a.len() > scan {
        &a[a.len() - scan..]
    } else {
        &a[..]
    };
    for head_len in [probe.min(b.len()), 1] {
        let head = &b[..head_len];
        let mut tried = 0;
        let mut k = find(tail, head, 0);
        while let Some(at) = k {
            tried += 1;
            if tried > max_candidates {
                break;
            }
            let n = tail.len() - at;
            if n <= b.len() && tail[at..] == b[..n] {
                return n;
            }
            k = find(tail, head, at + 1);
        }
        if head_len == 1 {
            break;
        }
    }
    0
}

fn find(haystack: &[u16], needle: &[u16], from: usize) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    (from..=haystack.len() - needle.len()).find(|&at| haystack[at..at + needle.len()] == *needle)
}
