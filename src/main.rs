use std::io::ErrorKind;
use reth_ecies::ECIESErrorImpl;
use reth_ecies::stream::ECIESStream;
use reth_eth_wire::{DisconnectReason, HelloMessage, UnauthedP2PStream};
use reth_network_peers::{NodeRecord, pk2id};
use secp256k1::{SECP256K1, SecretKey, rand};
use tokio::net::TcpStream;

//
// Sepolia (port 30405):
// enode://cfd3572bd7691fe03baf52106b873e01d9b5dca1714a74b316cb94151127dfd20adae3be559e3e6b44b78a5af1ed6f92ecc8676a2555fc7cdb2d29a0c37e1b2c@188.68.32.16:30405
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
    let tcp = TcpStream::connect((enode.address, enode.tcp_port)).await?;
    let ecies = match ECIESStream::connect(tcp, our_key, enode.id).await {
        Ok(s) => s,
        Err(e) => {
            match e.inner() {
                ECIESErrorImpl::IO(io) if matches!(io.kind(), ErrorKind::ConnectionReset | ErrorKind::UnexpectedEof) => {
                    eprintln!("{}:{} | peer closed during handshake (throttled or full)", enode.address, enode.tcp_port);
                }
                ECIESErrorImpl::IO(io) => eprintln!("{}:{} | network error: {io}", enode.address, enode.tcp_port),
                ECIESErrorImpl::TagCheckDecryptFailed | ECIESErrorImpl::InvalidAckData => {
                    eprintln!("{}:{} | key mismatch, enode ID is probably stale", enode.address, enode.tcp_port);
                }
                other => eprintln!("{}:{} | ECIES handshake failed: {other}", enode.address, enode.tcp_port),
            }
            return Ok(())
        }
    };

    let our_pub_key_as_peer_id = pk2id(&our_key.public_key(SECP256K1));

    let hello = HelloMessage::builder(our_pub_key_as_peer_id).build();

    let (mut p2p_stream, their_hello) = UnauthedP2PStream::new(ecies).handshake(hello).await?;
    let snap_versions: Vec<usize> = their_hello
        .capabilities
        .iter()
        .filter(|cap| cap.name == "snap")
        .map(|c| c.version)
        .collect();
    let snap: Option<usize> = snap_versions.iter().copied().max();
    println!("Their hello: {:?}", their_hello);

    match snap {
        Some(version) => {println!("Snap version: {:?}", version)}
        None => {println!("Snap: not advertised")}
    }

    if let Err(e) = p2p_stream.disconnect(DisconnectReason::ClientQuitting).await {
        eprintln!("Disconnect failed: {:?}", e);
    }

    Ok(())
}
