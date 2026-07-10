//! Interactive CLI for the artist-dht PoC.
//!
//! Starts a DHT node, optionally connects to other peers by ticket and lets
//! the user manage and search artists with slash commands.

use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;

use anyhow::Context;
use artist_dht::{
    Artist, ArtistDhtConfig, ArtistDhtEvent, ArtistDhtService, NetworkId, PeerTicket, SearchOutcome,
};
use clap::Parser;
use rustyline::ExternalPrinter;
use rustyline::error::ReadlineError;

#[derive(Debug, Parser)]
#[command(name = "artist-dht-cli", about = "Distributed artist directory PoC")]
struct Args {
    /// Directory for the peer identity and database.
    #[arg(long)]
    data_dir: PathBuf,

    /// Name of the network to join (all peers must use the same name).
    #[arg(long)]
    network_id: String,

    /// Display name of this peer (used only for the prompt).
    #[arg(long)]
    name: String,

    /// Ticket(s) of peers to connect to on startup; may be repeated.
    #[arg(long)]
    connect: Vec<String>,

    /// Enable verbose logging.
    #[arg(long)]
    verbose: bool,
}

/// A line of user input, or the request to stop.
enum Input {
    Line(String),
    Quit,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let filter = if args.verbose {
        "artist_dht=debug,federation_net=debug,info"
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

    let config = ArtistDhtConfig::builder()
        .data_dir(&args.data_dir)
        .network_id(NetworkId::from_name(&args.network_id))
        .build()
        .context("invalid configuration")?;

    let (service, mut events) = ArtistDhtService::start(config)
        .await
        .context("failed to start the DHT node")?;

    println!("Endpoint ID: {}", service.endpoint_id());
    println!("Node ID: {}", service.node_id());
    match service.ticket().await {
        Ok(ticket) => println!("Ticket: {ticket}"),
        Err(err) => eprintln!("Could not create a ticket yet: {err}"),
    }

    for ticket in &args.connect {
        let ticket: PeerTicket = match ticket.parse() {
            Ok(ticket) => ticket,
            Err(err) => {
                println!("Invalid ticket (make sure you copied the whole string): {err}");
                let _ = service.shutdown().await;
                std::process::exit(1);
            }
        };
        match service.connect(ticket).await {
            Ok(peer) => println!("Connected to: {peer}"),
            Err(err) => {
                println!("Connection rejected: {err}");
                let _ = service.shutdown().await;
                std::process::exit(1);
            }
        }
    }
    if args.connect.is_empty() {
        println!("Waiting for peers... (share the ticket above)");
    }
    println!("Type /help for commands.");

    // Rustyline is blocking, so it runs on its own thread and forwards lines
    // through a channel; the external printer keeps async output readable.
    let mut editor = rustyline::DefaultEditor::new().context("failed to init the line editor")?;
    // The external printer needs a TTY; when stdout is piped (e.g. through
    // `tee`), fall back to plain println.
    let mut printer = editor.create_external_printer().ok();
    let prompt = format!("{}> ", args.name);
    let (line_tx, line_rx) = std_mpsc::sync_channel::<Input>(16);
    // The reader waits for this ack after every line, so the next prompt is
    // drawn only after the command's output has been printed — otherwise the
    // line editor's redraws can visually swallow the output.
    let (ack_tx, ack_rx) = std_mpsc::channel::<()>();
    std::thread::spawn(move || {
        loop {
            match editor.readline(&prompt) {
                Ok(line) => {
                    let _ = editor.add_history_entry(&line);
                    if line_tx.send(Input::Line(line)).is_err() || ack_rx.recv().is_err() {
                        break;
                    }
                }
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                    let _ = line_tx.send(Input::Quit);
                    break;
                }
                Err(err) => {
                    eprintln!("input error: {err}");
                    let _ = line_tx.send(Input::Quit);
                    break;
                }
            }
        }
    });
    let (async_line_tx, mut async_line_rx) = tokio::sync::mpsc::channel::<Input>(16);
    std::thread::spawn(move || {
        while let Ok(input) = line_rx.recv() {
            if async_line_tx.blocking_send(input).is_err() {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            input = async_line_rx.recv() => {
                match input {
                    Some(Input::Line(line)) => {
                        let quit = handle_line(&service, line.trim()).await;
                        // Let the reader draw the next prompt.
                        let _ = ack_tx.send(());
                        if quit {
                            break;
                        }
                    }
                    Some(Input::Quit) | None => break,
                }
            }
            event = events.recv() => {
                match event {
                    Some(event) => {
                        let message = format_event(event);
                        match printer.as_mut() {
                            Some(printer) => {
                                let _ = printer.print(message);
                            }
                            None => println!("{message}"),
                        }
                    }
                    None => {
                        println!("Node stopped.");
                        break;
                    }
                }
            }
        }
    }

    service.shutdown().await.context("shutdown failed")?;
    println!("Bye.");
    // The rustyline thread keeps stdin busy; exit explicitly after a clean
    // shutdown.
    std::process::exit(0);
}

/// Executes one command line. Returns `true` when the user asked to quit.
async fn handle_line(service: &ArtistDhtService, line: &str) -> bool {
    let (command, rest) = match line.split_once(' ') {
        Some((command, rest)) => (command, rest.trim()),
        None => (line, ""),
    };
    match command {
        "" => {}
        "/help" => print_help(),
        "/quit" => return true,
        "/id" => {
            println!("Endpoint ID: {}", service.endpoint_id());
            println!("Node ID: {}", service.node_id());
        }
        "/ticket" => match service.ticket().await {
            Ok(ticket) => println!("Ticket: {ticket}"),
            Err(err) => println!("Could not create a ticket: {err}"),
        },
        "/peers" => {
            let peers = service.connected_peers();
            if peers.is_empty() {
                println!("No connected peers.");
            } else {
                println!("Connected peers: {}", peers.len());
                for peer in peers {
                    println!("  {peer}");
                }
            }
        }
        "/routing" => print_routing(service),
        "/list" => match service.list_local_artists().await {
            Ok(artists) if artists.is_empty() => println!("No local artists."),
            Ok(artists) => {
                println!("Local artists: {}", artists.len());
                for artist in artists {
                    println!("  {}  {}", short_id(&artist), artist.name);
                }
            }
            Err(err) => println!("Error: {err}"),
        },
        "/add" => {
            if rest.is_empty() {
                println!("Usage: /add <ARTIST_NAME>");
            } else {
                match service.add_artist(rest.to_string()).await {
                    Ok((artist, stats)) => {
                        println!("Added artist:");
                        println!("  ID: {}", artist.id);
                        println!("  Name: {}", artist.name);
                        println!("  Normalized: {}", artist.normalized_name);
                        println!(
                            "Published {} keys to {} DHT nodes{}.",
                            stats.keys,
                            stats.remote_nodes,
                            if stats.local_replica {
                                " (+ local replica)"
                            } else {
                                ""
                            }
                        );
                    }
                    Err(err) => println!("Error: {err}"),
                }
            }
        }
        "/delete" => {
            if rest.is_empty() {
                println!("Usage: /delete <ARTIST_ID>");
            } else {
                match service.resolve_local_artist_id(rest).await {
                    Ok(artist_id) => match service.delete_artist(artist_id).await {
                        Ok(stats) => {
                            println!("Deleted artist {artist_id}");
                            println!(
                                "Published tombstone to {} DHT nodes{}.",
                                stats.remote_nodes,
                                if stats.local_replica {
                                    " (+ local replica)"
                                } else {
                                    ""
                                }
                            );
                        }
                        Err(err) => println!("Error: {err}"),
                    },
                    Err(_) => println!("No unique local artist matches '{rest}'."),
                }
            }
        }
        "/search-local" => {
            if rest.is_empty() {
                println!("Usage: /search-local <QUERY>");
            } else {
                match service.search_local(rest).await {
                    Ok(artists) if artists.is_empty() => println!("No matches."),
                    Ok(artists) => {
                        for artist in artists {
                            println!("  {}  {}", short_id(&artist), artist.name);
                        }
                    }
                    Err(err) => println!("Error: {err}"),
                }
            }
        }
        "/search" => {
            if rest.is_empty() {
                println!("Usage: /search <QUERY>");
            } else {
                match service.search_network(rest).await {
                    Ok(outcome) => print_search(rest, &outcome),
                    Err(err) => println!("Error: {err}"),
                }
            }
        }
        "/republish" => match service.republish().await {
            Ok(stats) => println!(
                "Republished {} records ({} keys) to up to {} nodes.",
                stats.records, stats.keys, stats.remote_nodes
            ),
            Err(err) => println!("Error: {err}"),
        },
        other => println!("Unknown command: {other}. Type /help."),
    }
    false
}

fn print_help() {
    println!(
        "Commands:\n\
         \x20 /help                  show this help\n\
         \x20 /id                    show endpoint and node ids\n\
         \x20 /ticket                print the connection ticket\n\
         \x20 /peers                 list open connections\n\
         \x20 /routing               list known DHT contacts\n\
         \x20 /list                  list local artists\n\
         \x20 /add <ARTIST_NAME>     add and publish an artist\n\
         \x20 /delete <ARTIST_ID>    delete a local artist (id or hex prefix)\n\
         \x20 /search-local <QUERY>  search the local database only\n\
         \x20 /search <QUERY>        search locally and across the DHT\n\
         \x20 /republish             republish local records now\n\
         \x20 /quit                  shut down"
    );
}

fn print_routing(service: &ArtistDhtService) {
    let mut contacts = service.known_peers();
    if contacts.is_empty() {
        println!("No known DHT peers.");
        return;
    }
    contacts.sort_by_key(|contact| std::cmp::Reverse(contact.last_seen_ms));
    println!("Known DHT peers: {}", contacts.len());
    println!(
        "{:<15} {:<15} {:<10} Last seen",
        "Peer ID", "Node ID", "Connected"
    );
    let now = now_ms();
    for contact in contacts {
        let connected = if service.is_connected(contact.peer_id) {
            "yes"
        } else {
            "no"
        };
        let ago_s = now.saturating_sub(contact.last_seen_ms) / 1000;
        println!(
            "{:<15} {:<15} {:<10} {}s ago",
            shorten(&contact.peer_id.to_string()),
            shorten(&contact.node_id.to_string()),
            connected,
            ago_s
        );
    }
}

fn print_search(query: &str, outcome: &SearchOutcome) {
    println!("Search: {}", artist_dht::normalize_artist_name(query));
    println!();
    println!("Local results:");
    if outcome.local_results.is_empty() {
        println!("  No matches.");
    } else {
        for artist in &outcome.local_results {
            println!("  {}  {}", short_id(artist), artist.name);
        }
    }
    println!();
    println!("DHT results:");
    if outcome.network_results.is_empty() {
        println!("  No matches.");
    } else {
        for (i, artist) in outcome.network_results.iter().enumerate() {
            println!("  {}. {}", i + 1, artist.name);
            println!("     Artist ID: {}", artist.id);
            println!("     Owner: {}", artist.owner);
            println!("     Revision: {}", artist.revision);
        }
    }
    println!();
    println!("Lookup:");
    println!("  Queried nodes: {}", outcome.queried_nodes);
    println!("  Discovered nodes: {}", outcome.discovered_nodes);
    println!("  Duration: {} ms", outcome.duration.as_millis());
}

fn format_event(event: ArtistDhtEvent) -> String {
    match event {
        ArtistDhtEvent::PeerConnected { peer_id } => format!("Peer connected: {peer_id}"),
        ArtistDhtEvent::PeerDisconnected { peer_id } => {
            format!("Peer disconnected: {peer_id}")
        }
        ArtistDhtEvent::ContactDiscovered { contact } => {
            format!("Discovered DHT contact: {}", contact.peer_id)
        }
        ArtistDhtEvent::Error { message } => format!("Error: {message}"),
    }
}

fn short_id(artist: &Artist) -> String {
    shorten(&artist.id.to_string())
}

fn shorten(hex: &str) -> String {
    if hex.len() > 12 {
        format!("{}...", &hex[..12])
    } else {
        hex.to_string()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}
