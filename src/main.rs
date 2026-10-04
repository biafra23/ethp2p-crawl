use reth_ecies::stream::ECIESStream;
use reth_eth_wire::{HelloMessage, UnauthedEthStream, UnauthedP2PStream};
use reth_network_peers::NodeRecord;
use secp256k1::{SecretKey, rand};
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
    println!("Hello, world!");

    let enode: NodeRecord = std::env::args().nth(1).unwrap().parse()?;
    // record.id (PeerId), record.address (IpAddr), record.tcp_port

    println!("Parsed enode: {:?}", enode);

    let our_key = SecretKey::new(&mut rand::thread_rng());
    let tcp = TcpStream::connect((enode.address, enode.tcp_port)).await?;
    let ecies = ECIESStream::connect(tcp, our_key, enode.id).await?;

    let our_pub_key_as_peer_id = enode.id;

    let hello = HelloMessage::builder(our_pub_key_as_peer_id).build();

    let (p2p_stream, their_hello) = UnauthedP2PStream::new(ecies).handshake(hello).await?;
    let snap_versions: Vec<usize> = their_hello
        .capabilities
        .iter()
        .filter(|cap| cap.name == "snap")
        .map(|c| c.version)
        .collect();
    let snap: Option<usize> = snap_versions.iter().copied().max();
    println!("Their hello: {:?}", their_hello);
    println!("Snap: {:?}", snap);

    Ok(())
}
