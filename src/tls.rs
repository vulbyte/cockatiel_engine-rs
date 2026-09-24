//! TLS for the engine's WebSocket listener. The engine only accepts WSS://
//! connections: every client must complete a TLS handshake against the engine's
//! self-signed certificate before any Cockatiel frame is exchanged.
//!
//! A self-signed cert + key are generated once and persisted under the engine's
//! `tls/` directory so restarts reuse them (clients pin this exact cert via the
//! `COCKATIEL_TLS_CERT` env var the supervisor sets; if the engine regenerated
//! a fresh cert every boot, every client would have to re-fetch it).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

/// Directory (relative to the engine's cwd) holding the generated cert/key.
pub fn tls_dir() -> PathBuf {
    PathBuf::from("tls")
}

pub fn cert_path() -> PathBuf {
    tls_dir().join("cockatiel-cert.pem")
}

pub fn key_path() -> PathBuf {
    tls_dir().join("cockatiel-key.pem")
}

/// Load (or, on first run, generate and persist) the self-signed certificate +
/// key, then build a TLS acceptor from them. Returns the acceptor and the cert
/// path (so callers can surface it in logs).
pub fn build_tls_acceptor() -> Result<(TlsAcceptor, PathBuf), String> {
    let (cert_der, key_der) = ensure_cert()?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .map_err(|e| format!("failed to build TLS server config: {}", e))?;
    Ok((TlsAcceptor::from(Arc::new(config)), cert_path()))
}

fn ensure_cert() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), String> {
    let cert_path = cert_path();
    let key_path = key_path();

    if cert_path.exists() && key_path.exists() {
        let cert_der = load_cert(&cert_path)?;
        let key_der = load_key(&key_path)?;
        return Ok((cert_der, key_der));
    }

    // Generate a self-signed cert valid for the loopback names/IPs every
    // client uses. IP SANs are required — rustls verifies the hostname against
    // the cert, and an IP SAN is distinct from a DNS SAN.
    let mut params = rcgen::CertificateParams::new(vec![])
        .map_err(|e| format!("cert params: {}", e))?;
    params.subject_alt_names = vec![
        rcgen::SanType::DnsName(rcgen::Ia5String::try_from("localhost").map_err(|e| format!("san: {}", e))?),
        rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        rcgen::SanType::IpAddress(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)),
    ];
    let key_pair = rcgen::KeyPair::generate().map_err(|e| format!("key generation failed: {}", e))?;
    let certified = params
        .self_signed(&key_pair)
        .map_err(|e| format!("cert generation failed: {}", e))?;
    let cert_pem = certified.pem();
    let key_pem = key_pair.serialize_pem();

    std::fs::create_dir_all(tls_dir()).map_err(|e| format!("tls dir: {}", e))?;
    std::fs::write(&cert_path, &cert_pem).map_err(|e| format!("write cert: {}", e))?;
    std::fs::write(&key_path, &key_pem).map_err(|e| format!("write key: {}", e))?;

    let cert_der = load_cert(&cert_path)?;
    let key_der = load_key(&key_path)?;
    Ok((cert_der, key_der))
}

fn load_cert(path: &Path) -> Result<CertificateDer<'static>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read cert {}: {}", path.display(), e))?;
    let mut reader = std::io::BufReader::new(bytes.as_slice());
    rustls_pemfile::certs(&mut reader)
        .next()
        .ok_or_else(|| format!("no certificate in {}", path.display()))?
        .map_err(|e| format!("parse cert {}: {}", path.display(), e))
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read key {}: {}", path.display(), e))?;
    let mut reader = std::io::BufReader::new(bytes.as_slice());
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("parse key {}: {}", path.display(), e))?
        .ok_or_else(|| format!("no private key in {}", path.display()))
}