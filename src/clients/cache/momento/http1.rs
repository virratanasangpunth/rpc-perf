use core::panic;
use std::{sync::atomic::Ordering, time::Instant};

use super::{Config, REQUEST, REQUEST_UNSUPPORTED, RESPONSE_EX, RUNNING};
use crate::workload::{ClientRequest, ClientWorkItemKind};
use bytes::{Bytes, BytesMut};
use http::{Method, Version};
use http_body_util::{BodyExt, Full};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use protocol_memcache::SET_EX;
use tokio::runtime::Runtime;

use async_channel::Receiver;

const MB: usize = 1024 * 1024;

pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
) {
    for _ in 0..config.client().unwrap().poolsize() {
        let endpoints = config.target().endpoints();
        if endpoints.len() > 1 {
            panic!("We dont support multiple endpoints. {endpoints:?}");
        }
        let the_endpoint = endpoints.get(0).unwrap();
        runtime.spawn(task(
            work_receiver.clone(),
            the_endpoint.to_string(),
            config.clone(),
        ));
    }
}

#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
    endpoint: String,
    config: Config,
) -> Result<(), std::io::Error> {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let rustls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(rustls_config)
        .https_or_http()
        .enable_http1()
        .build();

    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new())
        .http1_max_buf_size(16 * MB)
        .build(connector.clone());

    let momento_api_key = std::env::var("MOMENTO_API_KEY").unwrap_or_else(|_| {
        eprintln!("environment variable `MOMENTO_API_KEY` is not set");
        std::process::exit(1);
    });

    let cache = config.target().cache_name().unwrap_or_else(|| {
        eprintln!("cache name is not specified in the `target` section");
        std::process::exit(1);
    });

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = match work_receiver.recv().await {
            Ok(w) => w,
            Err(_) => {
                continue;
            }
        };

        REQUEST.increment();

        match work_item {
            ClientWorkItemKind::Reconnect => {
                REQUEST_UNSUPPORTED.increment();
                continue;
            }
            ClientWorkItemKind::Request { request, sequence } => match request {
                ClientRequest::Get(_get) => todo!(),
                ClientRequest::Set(r) => {
                    let set_request = build_set_request(
                        &endpoint,
                        &momento_api_key,
                        cache,
                        std::str::from_utf8(&r.key).unwrap(),
                        r.value,
                    );

                    let start = Instant::now();

                    match client.request(set_request).await {
                        Ok(mut response) => {
                            let mut body = BytesMut::new();
                            while let Some(next) = response.frame().await {
                                if let Ok(frame) = next {
                                    if let Some(chunk) = frame.data_ref() {
                                        body.extend_from_slice(chunk);
                                    }
                                } else {
                                    SET_EX.increment();
                                    RESPONSE_EX.increment();
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            println!("error: {e:?}");
                            continue;
                        }
                    }
                }
                _ => {
                    REQUEST_UNSUPPORTED.increment();
                    continue;
                }
            },
        }
    }
    Ok(())
}

fn build_get_request(
    endpoint: &str,
    momento_api_key: &str,
    cache: &str,
    key: &str,
) -> http::Request<Full<Bytes>> {
    let uri = format!("{}/cache/{}?key={}", endpoint, cache, key);
    let empty_bytes: Bytes = Vec::new().into();

    http::Request::builder()
        .version(Version::HTTP_11)
        .method(Method::GET)
        .uri(uri)
        .header("authorization", momento_api_key)
        .body(Full::<Bytes>::new(empty_bytes))
        .unwrap()
}

fn build_set_request(
    endpoint: &str,
    momento_api_key: &str,
    cache: &str,
    key: &str,
    content: Bytes,
) -> http::Request<Full<Bytes>> {
    let uri = format!("{}/cache/{}?key={}ttl_seconds=86400", endpoint, cache, key);

    http::Request::builder()
        .version(Version::HTTP_11)
        .method(Method::PUT)
        .uri(uri)
        .header("authorization", momento_api_key)
        .body(Full::<Bytes>::new(content))
        .unwrap()
}
