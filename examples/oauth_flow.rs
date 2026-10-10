use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use pi_durable::ai::oauth::anthropic::{
    Endpoints, exchange_code, is_oauth_token, parse_authorization_input, refresh,
    resolve_manual_input,
};
use pi_durable::ai::oauth::callback::Listener;
use pi_durable::ai::oauth::{now_ms, pkce, query_param};
use pi_durable::cancel::CancelToken;

const RFC7636_VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const RFC7636_CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
const PROVIDER: &str = "Anthropic";
const AUTH_CODE: &str = "auth-code-1";
const ACCESS_1: &str = "sk-ant-oat01-first-access";
const REFRESH_1: &str = "sk-ant-ort01-first-refresh";
const ACCESS_2: &str = "sk-ant-oat01-second-access";
const REFRESH_2: &str = "sk-ant-ort01-second-refresh";
const EXPIRES_IN_S: i64 = 3600;

struct Expect {
    challenge: String,
    redirect_uri: String,
}

fn check<T: PartialEq + std::fmt::Debug>(label: &str, got: T, want: T) {
    assert!(got == want, "{label}: got {got:?}, want {want:?}");
}

fn main() {
    check(
        "rfc7636 challenge",
        pkce::challenge(RFC7636_VERIFIER),
        RFC7636_CHALLENGE.to_owned(),
    );
    check("is_oauth_token oat", is_oauth_token(ACCESS_1), true);
    check(
        "is_oauth_token api key",
        is_oauth_token("sk-ant-api03-x"),
        false,
    );
    check_parsing();

    let fake = TcpListener::bind("127.0.0.1:0").expect("fake server binds");
    let fake_port = fake.local_addr().expect("fake address").port();
    let base = format!("http://127.0.0.1:{fake_port}");
    let blocker = TcpListener::bind("127.0.0.1:0").expect("blocker binds");
    let taken_port = blocker.local_addr().expect("blocker address").port();
    let endpoints = Endpoints {
        authorize_url: format!("{base}/authorize"),
        token_url: format!("{base}/v1/oauth/token"),
        copy_code_redirect_uri: format!("{base}/oauth/code/callback"),
        callback_port: taken_port,
        ..Endpoints::default()
    };

    let pair = pkce::generate().expect("pkce");
    check(
        "pkce challenge",
        pkce::challenge(&pair.verifier),
        pair.challenge.clone(),
    );
    check("pkce verifier length", pair.verifier.len(), 43);

    let listener =
        Listener::bind(&endpoints.callback_host, endpoints.callback_port).expect("callback binds");
    check(
        "fallback port differs from taken",
        listener.port() != taken_port,
        true,
    );
    let redirect_uri = endpoints.loopback_redirect_uri(listener.port());
    let link = endpoints.authorize_link(&redirect_uri, &pair);
    let (link_base, link_query) = link.split_once('?').expect("authorize link has a query");
    check(
        "authorize base",
        link_base,
        format!("{base}/authorize").as_str(),
    );
    for (name, want) in [
        ("code", "true"),
        ("client_id", "9d1c250a-e61b-44d9-88ed-5944d1962f5e"),
        ("response_type", "code"),
        ("redirect_uri", redirect_uri.as_str()),
        ("code_challenge", pair.challenge.as_str()),
        ("code_challenge_method", "S256"),
        ("state", pair.verifier.as_str()),
    ] {
        check(
            &format!("authorize param {name}"),
            query_param(link_query, name).unwrap_or_default().as_str(),
            want,
        );
    }

    let expect = Expect {
        challenge: pair.challenge.clone(),
        redirect_uri: redirect_uri.clone(),
    };
    thread::spawn(move || serve_fake(fake, expect));

    let port = listener.port();
    let state = pair.verifier.clone();
    let browser = thread::spawn(move || {
        let bad = hit(port, "/callback?code=evil&state=not-the-verifier");
        let good = hit(port, &format!("/callback?code={AUTH_CODE}&state={state}"));
        (bad, good)
    });
    let code = listener
        .wait_code(PROVIDER, "/callback", &pair.verifier, &CancelToken::new())
        .expect("callback yields the code");
    let (bad, good) = browser.join().expect("browser thread");
    check("bad state page", bad.as_str(), "HTTP/1.1 400 Bad Request");
    check("good page", good.as_str(), "HTTP/1.1 200 OK");
    check("callback code", code.as_str(), AUTH_CODE);

    let before = now_ms();
    let first = exchange_code(
        &endpoints,
        &code,
        &pair.verifier,
        &pair.verifier,
        &redirect_uri,
        None,
    )
    .expect("exchange");
    let after = now_ms();
    check("exchange access", first.access.as_str(), ACCESS_1);
    check("exchange refresh", first.refresh.as_str(), REFRESH_1);
    let margin = EXPIRES_IN_S * 1000 - 5 * 60 * 1000;
    assert!(
        first.expires_ms >= before + margin && first.expires_ms <= after + margin,
        "expires_ms {} outside [{}, {}]",
        first.expires_ms,
        before + margin,
        after + margin
    );

    let second = refresh(&endpoints, &first.refresh, None).expect("refresh");
    check("refresh access", second.access.as_str(), ACCESS_2);
    check("refresh refresh", second.refresh.as_str(), REFRESH_2);

    let wrong = exchange_code(
        &endpoints,
        &code,
        "wrong-verifier",
        "wrong-verifier",
        &redirect_uri,
        None,
    );
    let wrong = wrong.expect_err("a wrong verifier is refused");
    assert!(
        wrong.starts_with("Token exchange request failed."),
        "{wrong}"
    );
    assert!(wrong.contains("status=400"), "{wrong}");
    let unknown = refresh(&endpoints, "sk-ant-ort01-never-issued", None)
        .expect_err("an unknown refresh token is refused");
    assert!(
        unknown.starts_with("Anthropic token refresh request failed."),
        "{unknown}"
    );

    let error_listener = Listener::bind("127.0.0.1", 0).expect("binds");
    let error_port = error_listener.port();
    let denied = thread::spawn(move || {
        hit(
            error_port,
            &format!(
                "/callback?error=access_denied&error_description=no+thanks&state={}",
                "x"
            ),
        );
    });
    let denied_error = error_listener
        .wait_code(PROVIDER, "/callback", "x", &CancelToken::new())
        .expect_err("provider error ends the wait");
    denied.join().expect("denied thread");
    check(
        "provider error",
        denied_error.as_str(),
        "Anthropic authorization failed: no thanks",
    );

    let cancel_listener = Listener::bind("127.0.0.1", 0).expect("binds");
    let cancel = CancelToken::new();
    let trigger = cancel.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        trigger.cancel();
    });
    let started = Instant::now();
    let cancelled = cancel_listener
        .wait_code(PROVIDER, "/callback", "x", &cancel)
        .expect_err("cancel ends the wait");
    let elapsed = started.elapsed();
    check("cancel error", cancelled.as_str(), "Login cancelled");
    assert!(elapsed < Duration::from_secs(1), "cancel took {elapsed:?}");

    println!("oauth flow ok: callback, exchange, refresh, refusals, cancel in {elapsed:?}");
}

fn check_parsing() {
    let cases = [
        ("", (None, None)),
        ("auth-code-1", (Some("auth-code-1"), None)),
        ("auth-code-1#st", (Some("auth-code-1"), Some("st"))),
        ("a#b#c", (Some("a"), Some("b"))),
        (
            "http://localhost:53692/callback?code=abc&state=st",
            (Some("abc"), Some("st")),
        ),
        (
            "https://x.example/cb?code=a%2Bb+c&state=s#frag",
            (Some("a+b c"), Some("s")),
        ),
        ("code=abc&state=st", (Some("abc"), Some("st"))),
        ("?code=abc&state=st", (Some("abc"), Some("st"))),
    ];
    for (input, (code, state)) in cases {
        let parsed = parse_authorization_input(input);
        check(
            &format!("parse {input:?}"),
            (parsed.code, parsed.state),
            (code.map(str::to_owned), state.map(str::to_owned)),
        );
    }
    check(
        "manual code#state",
        resolve_manual_input("auth-code-1#verifier", "verifier"),
        Ok(("auth-code-1".to_owned(), "verifier".to_owned())),
    );
    check(
        "manual state mismatch",
        resolve_manual_input("auth-code-1#other", "verifier"),
        Err("OAuth state mismatch".to_owned()),
    );
    check(
        "manual missing code",
        resolve_manual_input("#verifier", "verifier"),
        Err("Missing authorization code".to_owned()),
    );
}

fn hit(port: u16, target: &str) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to callback");
    write!(stream, "GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n").expect("send request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    response.lines().next().unwrap_or_default().to_owned()
}

fn serve_fake(listener: TcpListener, expect: Expect) {
    for stream in listener.incoming() {
        let mut stream = stream.expect("accept");
        let (head, body) = read_request(&mut stream);
        let request_line = head.lines().next().unwrap_or_default().to_owned();
        let (status, reply) = if request_line == "POST /v1/oauth/token HTTP/1.1" {
            token_reply(&body, &expect)
        } else {
            ("404 Not Found", "{}".to_owned())
        };
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
            reply.len()
        )
        .expect("reply");
    }
}

fn token_reply(body: &str, expect: &Expect) -> (&'static str, String) {
    let value: serde_json::Value = serde_json::from_str(body).expect("token body is JSON");
    let field = |name: &str| value[name].as_str().unwrap_or_default().to_owned();
    let client_ok = field("client_id") == "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
    let accepted = match field("grant_type").as_str() {
        "authorization_code" => {
            client_ok
                && field("code") == AUTH_CODE
                && field("state") == field("code_verifier")
                && pkce::challenge(&field("code_verifier")) == expect.challenge
                && field("redirect_uri") == expect.redirect_uri
        }
        "refresh_token" => client_ok && field("refresh_token") == REFRESH_1,
        _ => false,
    };
    if !accepted {
        return ("400 Bad Request", r#"{"error":"invalid_grant"}"#.to_owned());
    }
    let (access, refresh) = if field("grant_type") == "refresh_token" {
        (ACCESS_2, REFRESH_2)
    } else {
        (ACCESS_1, REFRESH_1)
    };
    let reply = serde_json::json!({
        "access_token": access,
        "refresh_token": refresh,
        "expires_in": EXPIRES_IN_S,
        "token_type": "bearer",
    });
    ("200 OK", reply.to_string())
}

fn read_request(stream: &mut TcpStream) -> (String, String) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(position) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        let read = stream.read(&mut chunk).expect("read request");
        assert!(read > 0, "client closed before the head ended");
        buf.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let length: usize = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().expect("content-length"))
        })
        .unwrap_or(0);
    while buf.len() < head_end + length {
        let read = stream.read(&mut chunk).expect("read body");
        assert!(read > 0, "client closed before the body ended");
        buf.extend_from_slice(&chunk[..read]);
    }
    let body = String::from_utf8_lossy(&buf[head_end..head_end + length]).into_owned();
    (head, body)
}
