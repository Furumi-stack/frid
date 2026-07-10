//! CLI demo for the federation-net P2P engine.
//!
//! Starts a peer, prints a shareable ticket and exchanges chat messages with
//! connected peers. Run two instances (optionally on different machines) and
//! pass the first peer's ticket to the second via `--connect`.

use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::Parser;
use federation_net::{
    ConnectionDirection, NetworkConfig, NetworkEngine, NetworkEvent, NetworkId, PeerTicket,
    SchemaId,
};
use tokio::io::{AsyncBufReadExt, BufReader};

/// Domain message type of the demo application.
///
/// The library knows nothing about this type; it is defined entirely here.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum DemoMessage {
    Text { sender: String, body: String },
    Ping { nonce: u64 },
}

/// Schema name of the demo message format.
const DEMO_SCHEMA: &str = "demo-message-v1";

#[derive(Debug, Parser)]
#[command(
    name = "federation-net-demo",
    about = "P2P chat demo for federation-net"
)]
struct Args {
    /// Directory for the persistent peer identity.
    #[arg(long)]
    data_dir: PathBuf,

    /// Human-readable name of the network to join.
    #[arg(long)]
    network_id: String,

    /// Display name used as the sender of chat messages.
    #[arg(long)]
    name: String,

    /// Ticket of a peer to connect to on startup.
    #[arg(long)]
    connect: Option<String>,

    /// Enable verbose logging.
    #[arg(long)]
    verbose: bool,
}

fn prompt(name: &str) {
    print!("{name}> ");
    let _ = std::io::stdout().flush();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let filter = if args.verbose {
        "federation_net=debug,federation_net_demo=debug,info"
    } else {
        "warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_writer(std::io::stderr)
        .init();

    let config = NetworkConfig::builder()
        .data_dir(&args.data_dir)
        .network_id(NetworkId::from_name(&args.network_id))
        .schema_id(SchemaId::from_name(DEMO_SCHEMA))
        .build()
        .context("invalid configuration")?;

    let (engine, mut events) = NetworkEngine::<DemoMessage>::start(config)
        .await
        .context("failed to start the network engine")?;

    println!("Endpoint ID: {}", engine.endpoint_id());
    println!("Network ID: {}", engine.network_id());
    println!("Schema ID: {}", engine.schema_id());
    match engine.ticket().await {
        Ok(ticket) => println!("Ticket: {ticket}"),
        Err(err) => eprintln!("Could not create a ticket yet: {err}"),
    }

    if let Some(ticket) = &args.connect {
        let ticket: PeerTicket = ticket.parse().context("invalid ticket")?;
        match engine.connect(ticket).await {
            Ok(peer_id) => {
                println!("Connected to: {peer_id}");
                println!("Type a message and press Enter.");
            }
            Err(err) => {
                println!("Connection rejected: {err}");
                let _ = engine.shutdown().await;
                std::process::exit(1);
            }
        }
    } else {
        println!("Waiting for peers...");
    }

    let name = args.name.clone();
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    prompt(&name);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!();
                break;
            }
            line = stdin.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        if handle_line(&engine, &name, line.trim()).await {
                            break;
                        }
                        prompt(&name);
                    }
                    Ok(None) => break, // stdin closed
                    Err(err) => {
                        eprintln!("failed to read stdin: {err}");
                        break;
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Some(event) => {
                        println!();
                        print_event(&name, event);
                        prompt(&name);
                    }
                    None => {
                        println!("Engine stopped.");
                        break;
                    }
                }
            }
        }
    }

    engine.shutdown().await.context("shutdown failed")?;
    println!("Bye.");
    // The blocking thread behind `tokio::io::stdin()` keeps the runtime alive
    // until stdin closes; the engine is already shut down, so exit directly.
    let _ = std::io::stdout().flush();
    std::process::exit(0);
}

/// Handles one line of user input. Returns `true` when the user asked to quit.
async fn handle_line(engine: &NetworkEngine<DemoMessage>, name: &str, line: &str) -> bool {
    match line {
        "" => false,
        "/quit" => true,
        "/peers" => {
            let peers = engine.connected_peers();
            if peers.is_empty() {
                println!("No connected peers.");
            } else {
                for peer in peers {
                    println!("{peer}");
                }
            }
            false
        }
        "/ticket" => {
            match engine.ticket().await {
                Ok(ticket) => println!("Ticket: {ticket}"),
                Err(err) => println!("Could not create a ticket: {err}"),
            }
            false
        }
        "/ping" => {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or_default();
            broadcast(engine, DemoMessage::Ping { nonce }).await;
            false
        }
        body => {
            broadcast(
                engine,
                DemoMessage::Text {
                    sender: name.to_string(),
                    body: body.to_string(),
                },
            )
            .await;
            false
        }
    }
}

/// Sends a message to every connected peer.
async fn broadcast(engine: &NetworkEngine<DemoMessage>, message: DemoMessage) {
    let peers = engine.connected_peers();
    if peers.is_empty() {
        println!("No connected peers.");
        return;
    }
    for peer in peers {
        if let Err(err) = engine.send(peer, &message).await {
            println!("failed to send to {peer}: {err}");
        }
    }
}

fn print_event(name: &str, event: NetworkEvent<DemoMessage>) {
    match event {
        NetworkEvent::PeerConnected { peer_id, direction } => {
            let direction = match direction {
                ConnectionDirection::Incoming => "incoming",
                ConnectionDirection::Outgoing => "outgoing",
            };
            println!("Peer connected ({direction}): {peer_id}");
        }
        NetworkEvent::PeerDisconnected { peer_id, reason } => match reason {
            Some(reason) => println!("Peer disconnected: {peer_id} ({reason})"),
            None => println!("Peer disconnected: {peer_id}"),
        },
        NetworkEvent::MessageReceived { peer_id, message } => match message {
            DemoMessage::Text { body, .. } => {
                println!("{name} received from {peer_id}: {body}");
            }
            DemoMessage::Ping { nonce } => {
                println!("{name} received ping from {peer_id} (nonce {nonce})");
            }
        },
        NetworkEvent::ProtocolError { peer_id, error } => match peer_id {
            Some(peer_id) => println!("Protocol error with {peer_id}: {error}"),
            None => println!("Protocol error: {error}"),
        },
    }
}
