use futures::{SinkExt, StreamExt};
use reth_chainspec::{Head, MAINNET, SEPOLIA};
use reth_ecies::ECIESErrorImpl;
use reth_ecies::stream::ECIESStream;
use reth_eth_wire::errors::{P2PHandshakeError, P2PStreamError};
use reth_eth_wire::protocol::Protocol;
use reth_eth_wire::{
    Capability, EthMessage, EthNetworkPrimitives, P2PStream, ProtocolMessage, UnifiedStatus,
};
use reth_eth_wire::{DisconnectReason, HelloMessage, UnauthedP2PStream};
use reth_network_peers::{NodeRecord, pk2id};
use secp256k1::{SECP256K1, SecretKey, rand};
use std::io::ErrorKind;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;

//
// Sepolia (port 30405):
// mine:
// enode://cfd3572bd7691fe03baf52106b873e01d9b5dca1714a74b316cb94151127dfd20adae3be559e3e6b44b78a5af1ed6f92ecc8676a2555fc7cdb2d29a0c37e1b2c@188.68.32.16:30405
// others
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let enode: NodeRecord = std::env::args().nth(1).unwrap().parse()?;
    // record.id (PeerId), record.address (IpAddr), record.tcp_port

    let our_key = SecretKey::new(&mut rand::thread_rng());
    let tcp = match TcpStream::connect((enode.address, enode.tcp_port)).await {
        Ok(tcp) => tcp,
        Err(e) => {
            eprintln!("{}:{} | failed to connect: {}", enode.address, enode.tcp_port, e);
            return Ok(())
        }
    };
    let ecies = match ECIESStream::connect(tcp, our_key, enode.id).await {
        Ok(s) => s,
        Err(e) => {
            match e.inner() {
                ECIESErrorImpl::IO(io)
                    if matches!(
                        io.kind(),
                        ErrorKind::ConnectionReset | ErrorKind::UnexpectedEof
                    ) =>
                {
                    eprintln!(
                        "{}:{} | peer closed during handshake (throttled or full)",
                        enode.address, enode.tcp_port
                    );
                }
                ECIESErrorImpl::IO(io) => {
                    eprintln!("{}:{} | network error: {io}", enode.address, enode.tcp_port)
                }
                ECIESErrorImpl::TagCheckDecryptFailed | ECIESErrorImpl::InvalidAckData => {
                    eprintln!(
                        "{}:{} | key mismatch, enode ID is probably stale",
                        enode.address, enode.tcp_port
                    );
                }
                other => eprintln!(
                    "{}:{} | ECIES handshake failed: {other}",
                    enode.address, enode.tcp_port
                ),
            }
            return Ok(());
        }
    };

    let our_id = pk2id(&our_key.public_key(SECP256K1));

    let mut hello = HelloMessage::builder(our_id)
        .client_version("myotis/crawl")
        .build();
    hello.try_add_protocol(SnapVersion::V1.into()).ok();

    let (mut p2p_stream, their_hello) = match UnauthedP2PStream::new(ecies).handshake(hello).await {
        Ok(pair) => pair,
        Err(P2PStreamError::HandshakeError(P2PHandshakeError::Disconnected(reason))) => {
            eprintln!(
                "L83: {}:{} | peer disconnected during handshake: {reason}",
                enode.address, enode.tcp_port
            );
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    // Handshake done

    let caps = p2p_stream.shared_capabilities();
    let eth_version = caps.eth_version()?;
    let snap_off = caps.find(&SnapVersion::V1.capability()).map(|c| c.relative_message_id_offset()); // None => peer didn't share snap

    // let spec = match chain_arg { "sepolia" => SEPOLIA.as_ref(), _ => MAINNET.as_ref() };
    let spec = SEPOLIA.as_ref(); //SEPOLIA
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let head = Head { number: 1_735_371, timestamp: now, ..Default::default() }; //SEPOLIA
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

    match p2p_stream.next().await {
        Some(Ok(message)) => {
            let theirs = UnifiedStatus::from_message(
                ProtocolMessage::<EthNetworkPrimitives>::decode_status(
                    eth_version,
                    &mut &message[..],
                )?,
            );

            println!(
                "chain={} genesis={} fork={:?} head={} range={:?}..{:?}",
                theirs.chain,
                theirs.genesis,
                theirs.forkid,
                theirs.blockhash,
                theirs.earliest_block,
                theirs.latest_block
            );
        }
        Some(Err(e)) => {
            eprintln!("Error receiving message: {:?}", e);
        }
        None => {
            eprintln!("Stream closed");
        }
    }

    //.ok_or("closed")??;

    // a) support snap
    // -> make a snap query

    // b) does not support snap
    // -> disconnect

    // Disconnect
    if let Err(e) = p2p_stream.disconnect(DisconnectReason::ClientQuitting).await {
        eprintln!("Disconnect failed: {:?}", e);
    }

    Ok(())
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
