use alloy_primitives::{B256, ChainId};
use anyhow::{Context, anyhow};
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use reth_chainspec::{Head, MAINNET, SEPOLIA};
use reth_ecies::ECIESErrorImpl;
use reth_ecies::stream::ECIESStream;
use reth_eth_wire::errors::{P2PHandshakeError, P2PStreamError};
use reth_eth_wire::message::RequestPair;
use reth_eth_wire::protocol::Protocol;
use reth_eth_wire::{
    AccountRangeMessage, Capability, EthMessage, EthMessageID, EthNetworkPrimitives, EthVersion,
    GetAccountRangeMessage, GetBlockHeaders, HeadersDirection, P2PStream, ProtocolMessage,
    SnapMessageId, UnifiedStatus,
};
use reth_eth_wire::{DisconnectReason, HelloMessage, UnauthedP2PStream};
use reth_network_peers::{NodeRecord, pk2id};
use secp256k1::{SECP256K1, SecretKey, rand};
use std::fs::File;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

use reth_discv4::{DiscoveryUpdate, Discv4, Discv4ConfigBuilder};
use reth_network_peers::sepolia_nodes;
use serde::Serialize;
use std::collections::HashSet;
use tokio::sync::Semaphore;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{EnvFilter, fmt};

use clap::Parser;
use std::path::{Path, PathBuf};

type P2p = P2PStream<ECIESStream<TcpStream>>;
//
// Sepolia (port 30405):
// mine:
// enode://cfd3572bd7691fe03baf52106b873e01d9b5dca1714a74b316cb94151127dfd20adae3be559e3e6b44b78a5af1ed6f92ecc8676a2555fc7cdb2d29a0c37e1b2c@188.68.32.16:30405
// others (sepolia)
// enode://4e5e92199ee224a01932a377160aa432f31d0b351f84ab413a8e0a42f4f36476f8fb1cbe914af0d9aef0d51665c214cf653c651c4bbd9d5550a934f241f1682b@138.197.51.181:30303
// enode://143e11fb766781d22d92a2e33f8f104cddae4411a122295ed1fdb6638de96a6ce65f5b7c964ba3763bba27961738fef7d3ecc739268f3e5e771fb4c87b6234ba@146.190.1.103:30303
// enode://8b61dc2d06c3f96fddcbebb0efb29d60d3598650275dc469c22229d3e5620369b0d3dedafd929835fe7f489618f19f456fe7c0df572bf2d914a9f4e006f783a9@170.64.250.88:30303
// enode://10d62eff032205fcef19497f35ca8477bea0eadfff6d769a147e895d8b2b8f8ae6341630c645c30f5df6e67547c03494ced3d9c5764e8622a26587b083b028e8@139.59.49.206:30303
// enode://9e9492e2e8836114cc75f5b929784f4f46c324ad01daf87d956f98b3b6c5fcba95524d6e5cf9861dc96a2c8a171ea7105bb554a197455058de185fa870970c7c@138.68.123.152:30303
//
// Mainnet (port 30406):
// enode://b317a1cc0713ff3fbd1f7207c5b12ac8a1168c5e3adf14b21f09d558b3e1066dd41f0bc82abeaa3f29c411fc25c488a7140262f8bbf3e0747631a073299c76cb@188.68.32.16:30406
//
// Gnosis (port 30407):
// enode://e0f6d12b6259591a421ec73f2254419cabfed7509d173dc20f44823d155a390afc3dc37bf2b375209fdb9509f88ca7321727f88d3ffdcbeeaa129b604eec030d@188.68.32.16:30407
//
// Address: 188.68.32.16 is the netcup relay, which forwards these ports to zbox. From this machine itself, dial 127.0.0.1 with the same key and port.
// Pinned in the repo: only the Sepolia enode is pinned on main (in NetworkConfig.java and rust/myotis-net/src/el/reader.rs), and it matches the live one. The mainnet and Gnosis enodes are not pinned anywhere.
fn init_logging() {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,ethp2p_crawl=debug")),
        )
        .with_file(false)
        .with_line_number(false)
        .with_target(false)
        // .with_ansi(false)
        // .with_timer(fmt::time::UtcTime::rfc_3339())   // needs the "time" feature; or drop this line for the default local-ish timestamp
        .event_format(
            fmt::format()
                .with_file(true) // …and on again inside the format
                .with_line_number(true)
                .compact(),
        )
        .init();
}

async fn probe(enode: NodeRecord, our_key: SecretKey) -> anyhow::Result<ProbeOutcome> {
    let tcp =
        match timeout(Duration::from_secs(5), TcpStream::connect((enode.address, enode.tcp_port)))
            .await
        {
            Ok(Ok(tcp)) => tcp,
            Ok(Err(e)) => return Ok(ProbeOutcome::Unreachable(e.kind())),
            Err(_) => return Ok(ProbeOutcome::Unreachable(ErrorKind::TimedOut)),
        };

    let ecies = match within(10, "ecies", ECIESStream::connect(tcp, our_key, enode.id)).await? {
        Ok(s) => s,
        Err(e) => {
            return Ok(match e.inner() {
                ECIESErrorImpl::IO(io)
                    if matches!(
                        io.kind(),
                        ErrorKind::ConnectionReset | ErrorKind::UnexpectedEof
                    ) =>
                {
                    ProbeOutcome::Reset
                }
                ECIESErrorImpl::IO(io) => ProbeOutcome::Unreachable(io.kind()),
                ECIESErrorImpl::TagCheckDecryptFailed | ECIESErrorImpl::InvalidAckData => {
                    ProbeOutcome::KeyMismatch
                }
                other => return Err(anyhow!("ecies: {other}")),
            });
        }
    };

    let our_id = pk2id(&our_key.public_key(SECP256K1));
    let mut our_hello = HelloMessage::builder(our_id).client_version("myotis/crawl").build();
    our_hello.try_add_protocol(SnapVersion::V1.into()).ok();

    let (mut p2p_stream, hello) = match UnauthedP2PStream::new(ecies).handshake(our_hello).await {
        Ok(pair) => pair,
        Err(e) => {
            return match disconnect_reason(&e) {
                Some(reason) => Ok(ProbeOutcome::Disconnected { reason, hello: None }),
                None => Err(e).context("p2p handshake"),
            };
        }
    };

    let caps = p2p_stream.shared_capabilities();
    let eth_version = caps.eth_version()?;
    let snap_off = caps.find(&SnapVersion::V1.capability()).map(|c| c.relative_message_id_offset()); // None => peer didn't share snap

    // let spec = match chain_arg { "sepolia" => SEPOLIA.as_ref(), _ => MAINNET.as_ref() };
    let spec = SEPOLIA.as_ref(); //SEPOLIA
    let head = Head {
        number: 10_000_000,
        timestamp: now(),
        hash: spec.genesis_hash(),
        ..Default::default()
    }; //SEPOLIA
    let mut status = UnifiedStatus::spec_builder(spec, &head);
    status.set_eth_version(eth_version);

    // Send status
    p2p_stream
        .send(
            alloy_rlp::encode(ProtocolMessage::<EthNetworkPrimitives>::from(EthMessage::Status(
                status.into_message(),
            )))
            .into(),
        )
        .await?;

    let frame = match within(10, "status", p2p_stream.next()).await? {
        Some(Ok(frame)) => frame,
        Some(Err(e)) => {
            return match disconnect_reason(&e) {
                Some(reason) => Ok(ProbeOutcome::Disconnected { reason, hello: Some(hello) }),
                None => Err(e).context("reading status"),
            };
        }
        None => return Err(anyhow!("stream closed before status")),
    };

    let their_status = UnifiedStatus::from_message(
        ProtocolMessage::<EthNetworkPrimitives>::decode_status(eth_version, &mut &frame[..])
            .context("decode status")?,
    );

    // debug!("theirs.forkId: {:?} status.forkId: {:?}", theirs.forkid, status.forkid);

    if their_status.chain.id() != 11155111 {
        return Ok(ProbeOutcome::Probed {
            hello: hello,
            status: their_status,
            snap: SnapCheck::WrongChain,
        });
    }
    let snap = match snap_off {
        None => SnapCheck::NotShared,
        Some(off) => snap_check(&mut p2p_stream, eth_version, off, their_status.blockhash).await?,
    };

    // Disconnect
    if let Err(e) = p2p_stream.disconnect(DisconnectReason::ClientQuitting).await {
        debug!("Disconnect failed: {:?}", e);
    }
    Ok(ProbeOutcome::Probed { hello: hello, status: their_status, snap: snap })
}

pub fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

#[derive(Debug)]
pub enum ProbeOutcome {
    Unreachable(ErrorKind), // TCP: refused / timed out / host unreachable
    Reset,                  // ECIES: peer closed (throttled or full)
    KeyMismatch,            // ECIES: enode ID is stale
    Disconnected { reason: DisconnectReason, hello: Option<HelloMessage> },
    Probed { hello: HelloMessage, status: UnifiedStatus, snap: SnapCheck },
}

#[derive(Debug)]
pub enum SnapCheck {
    NotShared,                                      // no snap in shared capabilities
    Served { accounts: usize, proof_nodes: usize }, // real data for the head root
    Empty,                                          // answered, but nothing for that root
    Timeout,
    Disconnected { reason: DisconnectReason, stage: Stage },
    WrongChain, // not Sepolia
}
#[derive(Debug)]
pub enum Stage {
    Headers,
    AccountRange,
}
use std::io::Write;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let args = Args::parse();

    let our_key = SecretKey::new(&mut rand::thread_rng()); // same key as the probe uses
    let local = NodeRecord::from_secret_key("0.0.0.0:0".parse()?, &our_key);
    let (tx, rx) = tokio::sync::mpsc::channel::<NodeRecord>(1024);
    let mut seen = HashSet::new(); // moved out of the task

    let mut cfg = Discv4ConfigBuilder::default();
    cfg.add_boot_nodes(sepolia_nodes());

    if let Some(seed_file_name) = args.seeds {
        debug!("seed-file: {:?}", seed_file_name);
        let seeds = load_seeds(&seed_file_name)?;
        cfg.add_boot_nodes(seeds.clone());
        // seen.insert(seed_file_name);
        for seed in seeds {
            if seen.insert(seed.id) {
                tx.send(seed).await?;
            }
        }
    }
    cfg.lookup_interval(Duration::from_secs(5)).external_ip_resolver(None); // skip the STUN/UPnP dance

    let discv4 = Discv4::spawn(local.udp_addr(), local, our_key, cfg.build()).await?;
    let mut updates = discv4.update_stream().await?;

    for enode in args.enodes {
        let seed: NodeRecord = enode; // enode://…@host:port
        debug!("enodes: {:?}", seed);
        seen.insert(seed.id); // so discovery doesn't queue it twice
        tx.send(seed).await?; // probed right away
    }
    tokio::spawn(async move {
        while let Some(u) = updates.next().await {
            // debug!("update: {:?}", &u);
            match u {
                DiscoveryUpdate::Added(n) | DiscoveryUpdate::DiscoveredAtCapacity(n) => {
                    if seen.insert(n.id) {
                        let _ = tx.send(n).await;
                    }
                }
                DiscoveryUpdate::EnrForkId(n, fork_id) => { /* see §5 */ }
                DiscoveryUpdate::Batch(us) => {
                    for u in us {
                        if let DiscoveryUpdate::Added(n)
                        | DiscoveryUpdate::DiscoveredAtCapacity(n) = u
                        {
                            if seen.insert(n.id) {
                                let _ = tx.send(n).await;
                            }
                        }
                    }
                }
                DiscoveryUpdate::Removed(_) => {}
            }
        }
    });

    let sem = Arc::new(Semaphore::new(16));
    let results = Arc::new(Mutex::new(File::create("results.jsonl")?));
    let mut rx = rx;

    while let Some(enode) = rx.recv().await {
        let permit = sem.clone().acquire_owned().await?;
        let results = results.clone();
        tokio::spawn(async move {
            let enode_str = enode.to_string();
            let outcome = timeout(Duration::from_secs(65), probe(enode, our_key)).await;
            if let Ok(Ok(ProbeOutcome::Probed { hello, status, snap })) = &outcome {
                // debug!("ProbeOutcome::Probed");
                // debug!("Client: {:?}, ProbeOutcome::Probed.snap={:?}", hello.client_version, snap);
                // debug!("Client: {:?}, ProbeOutcome::Probed.snap={:?}", hello., snap);
                debug!("Chain: {:?}, ProbeOutcome::Probed.snap={:?}", status.chain.id(), snap);
            }

            let line = Record { enode: enode_str, ts: now(), outcome: to_outcome(outcome) };
            // let line = Record { enode, ts: now(), outcome: outcome.into() };
            writeln!(results.lock().unwrap(), "{}", serde_json::to_string(&line).unwrap()).ok();
            drop(permit);
        });
    }

    // tokio::time::sleep(Duration::from_millis(50000)).await;

    // let enode: NodeRecord = std::env::args().nth(1).unwrap().parse()?;
    // // record.id (PeerId), record.address (IpAddr), record.tcp_port
    //
    // match tokio::time::timeout(Duration::from_secs(65), probe(enode)).await {
    //     Ok(Ok(outcome)) => println!("{outcome:?}"),
    //     Ok(Err(e)) => eprintln!("probe failed: {e:#}"),
    //     Err(_) => eprintln!("probe timed out"),
    // }
    Ok(())
}

#[derive(Serialize)]
struct Record {
    enode: String, // NodeRecord's Display gives the enode:// form
    ts: u64,
    outcome: Outcome, // a serialisable mirror of ProbeOutcome
}

#[derive(Serialize, Default)]
struct Outcome {
    kind: &'static str, // probed | disconnected | unreachable | reset | key_mismatch | error | timeout
    client: Option<String>,
    caps: Vec<String>,
    chain: Option<u64>,
    fork_hash: Option<String>,
    latest: Option<u64>,
    snap: Option<String>,
    reason: Option<String>,
}

fn to_outcome(r: Result<anyhow::Result<ProbeOutcome>, tokio::time::error::Elapsed>) -> Outcome {
    let mut o = Outcome::default();
    match r {
        Err(_) => o.kind = "timeout",
        Ok(Err(e)) => {
            o.kind = "error";
            o.reason = Some(format!("{e:#}"));
        }
        Ok(Ok(p)) => match p {
            ProbeOutcome::Unreachable(k) => {
                o.kind = "unreachable";
                o.reason = Some(format!("{k:?}"));
            }
            ProbeOutcome::Reset => o.kind = "reset",
            ProbeOutcome::KeyMismatch => o.kind = "key_mismatch",
            ProbeOutcome::Disconnected { reason, hello } => {
                o.kind = "disconnected";
                o.reason = Some(format!("{reason:?}"));
                if let Some(h) = hello {
                    fill_hello(&mut o, &h);
                }
            }
            ProbeOutcome::Probed { hello, status, snap } => {
                o.kind = "probed";
                fill_hello(&mut o, &hello);
                o.chain = Some(status.chain.id());
                o.fork_hash = Some(format!("{:?}", status.forkid.hash));
                o.latest = status.latest_block;
                o.snap = Some(format!("{snap:?}"));
            }
        },
    }
    o
}

fn fill_hello(o: &mut Outcome, h: &HelloMessage) {
    o.client = Some(h.client_version.clone());
    o.caps = h.capabilities.iter().map(|c| c.to_string()).collect();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum SnapVersion {
    V1 = 1,
    V2 = 2,
}

impl SnapVersion {
    pub const fn message_count(self) -> u8 {
        match self {
            Self::V1 => 8,
            Self::V2 => 10,
        }
    }
    pub const fn capability(self) -> Capability {
        Capability::new_static("snap", self as usize)
    }
    pub const fn protocol(self) -> Protocol {
        Protocol::new(self.capability(), self.message_count())
    }
}

impl From<SnapVersion> for Protocol {
    fn from(v: SnapVersion) -> Self {
        v.protocol()
    }
}

async fn within<T>(secs: u64, what: &str, fut: impl Future<Output = T>) -> anyhow::Result<T> {
    timeout(Duration::from_secs(secs), fut).await.map_err(|_| anyhow!("{what}: timed out"))
}

fn disconnect_reason(e: &P2PStreamError) -> Option<DisconnectReason> {
    match e {
        P2PStreamError::Disconnected(r) => Some(*r),
        P2PStreamError::HandshakeError(P2PHandshakeError::Disconnected(r)) => Some(*r),
        _ => None,
    }
}
use alloy_rlp::{Decodable, Encodable};
use tracing::debug;
use tracing_subscriber::fmt::init;

async fn snap_check(
    p2p: &mut P2p,
    eth_version: EthVersion,
    snap_off: u8,
    head_hash: B256,
) -> anyhow::Result<SnapCheck> {
    // 1. head header → state root
    let req = EthMessage::<EthNetworkPrimitives>::GetBlockHeaders(RequestPair {
        request_id: 1,
        message: GetBlockHeaders {
            start_block: head_hash.into(),
            limit: 1,
            skip: 0,
            direction: HeadersDirection::Rising,
        },
    });
    p2p.send(alloy_rlp::encode(ProtocolMessage::from(req)).into())
        .await
        .context("send GetBlockHeaders")?;

    let state_root = match wait_for(p2p, EthMessageID::BlockHeaders.to_u8(), 10).await? {
        Recv::Timeout => return Ok(SnapCheck::Timeout),
        Recv::Disconnected(r) => {
            return Ok(SnapCheck::Disconnected { reason: r, stage: Stage::Headers });
        }
        Recv::Frame(frame) => {
            let msg = ProtocolMessage::<EthNetworkPrimitives>::decode_message(
                eth_version,
                &mut &frame[..],
            )
            .context("decode BlockHeaders")?;
            match msg.message {
                EthMessage::BlockHeaders(RequestPair { request_id: 1, message }) => {
                    message.0.into_iter().next().context("empty BlockHeaders")?.state_root
                }
                other => return Err(anyhow!("unexpected eth message: {other:?}")),
            }
        }
    };

    // 2. GetAccountRange at that root
    let req = GetAccountRangeMessage {
        request_id: 2,
        root_hash: state_root,
        starting_hash: B256::ZERO,
        limit_hash: B256::repeat_byte(0xff),
        response_bytes: 1024,
    };
    let mut frame = vec![snap_off + SnapMessageId::GetAccountRange as u8];
    req.encode(&mut frame);
    p2p.send(frame.into()).await.context("send GetAccountRange")?;

    match wait_for(p2p, snap_off + SnapMessageId::AccountRange as u8, 10).await? {
        Recv::Timeout => Ok(SnapCheck::Timeout),
        Recv::Disconnected(r) => {
            Ok(SnapCheck::Disconnected { reason: r, stage: Stage::AccountRange })
        }
        Recv::Frame(frame) => {
            let resp =
                AccountRangeMessage::decode(&mut &frame[1..]).context("decode AccountRange")?;
            if resp.request_id != 2 {
                return Err(anyhow!("AccountRange for unknown request id {}", resp.request_id));
            }
            if resp.accounts.is_empty() {
                Ok(SnapCheck::Empty)
            } else {
                Ok(SnapCheck::Served {
                    accounts: resp.accounts.len(),
                    proof_nodes: resp.proof.len(),
                })
            }
        }
    }
}
enum Recv {
    Frame(BytesMut),
    Disconnected(DisconnectReason),
    Timeout,
}

async fn wait_for(p2p: &mut P2p, want_id: u8, secs: u64) -> anyhow::Result<Recv> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let tick = tokio::time::sleep(Duration::from_millis(500));
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => return Ok(Recv::Timeout),
            _ = tick => { p2p.flush().await.context("flush")?; },
            item = p2p.next() => {
                p2p.flush().await.context("flush")?;
                match item {
                None => return Err(anyhow!("stream closed")),
                Some(Err(e)) => return match disconnect_reason(&e) {
                    Some(r) => Ok(Recv::Disconnected(r)),
                    None => Err(e).context("reading frame"),
                },
                Some(Ok(frame)) if frame.first() == Some(&want_id) => return Ok(Recv::Frame(frame)),
                Some(Ok(_)) => {}
            }
            },
        }
    }
}

#[derive(Parser)]
struct Args {
    /// File with one enode per line; '#' starts a comment
    #[arg(long)]
    seeds: Option<PathBuf>,
    /// Enodes to probe right away
    enodes: Vec<NodeRecord>,
}

fn load_seeds(path: &Path) -> anyhow::Result<Vec<NodeRecord>> {
    std::fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?
        .lines()
        .map(|l| l.split('#').next().unwrap_or("").trim())
        .filter(|l| !l.is_empty())
        .map(|l| l.parse::<NodeRecord>().with_context(|| format!("bad enode: {l}")))
        .collect()
}
