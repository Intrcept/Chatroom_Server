use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

// start with cargo run --bin chatroom_server

const ADDRESS: &str = "0.0.0.0:3500";
const MAX_LINE_BYTES: usize = 8192;

type SharedWriter = Arc<Mutex<TcpStream>>;

#[derive(Default)]
struct Room {
    members: HashMap<String, SharedWriter>,
}

enum ReadLine {
    Complete(Vec<u8>),
    TooLong(Vec<u8>),
    Closed,
}

enum Command<'a> {
    Join(&'a str),
    Exit,
    Public(&'a str),
    Private {
        recipient: &'a str,
        message: &'a str,
    },
    Invalid,
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind(ADDRESS)?;
    println!("Chatroom server listening on {ADDRESS}");
    let room = Arc::new(Mutex::new(Room::default()));

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let address = match stream.peer_addr() {
                    Ok(address) => address,
                    Err(error) => {
                        eprintln!("Could not determine client address: {error}");
                        continue;
                    }
                };
                println!("Client connected: {address}");

                let client_room = Arc::clone(&room);
                thread::spawn(move || handle_client(stream, address, client_room));
            }
            Err(error) => eprintln!("Could not accept client connection: {error}"),
        }
    }

    Ok(())
}

fn handle_client(stream: TcpStream, address: SocketAddr, room: Arc<Mutex<Room>>) {
    let writer = match stream.try_clone() {
        Ok(stream) => Arc::new(Mutex::new(stream)),
        Err(error) => {
            eprintln!("{address}: could not clone client stream: {error}");
            return;
        }
    };
    let mut reader = BufReader::new(stream);
    let mut username = None;

    loop {
        match read_protocol_line(&mut reader) {
            Ok(ReadLine::Complete(mut bytes)) => {
                bytes.truncate(bytes.len() - 2);
                let text = String::from_utf8_lossy(&bytes);
                println!("{address} -> {text}\r");

                let keep_connection = match std::str::from_utf8(&bytes) {
                    Ok(line) => handle_command(line, &mut username, &writer, &room),
                    Err(_) => send_status(&writer, 500),
                };
                if !keep_connection {
                    break;
                }
            }
            Ok(ReadLine::TooLong(bytes)) => {
                let text = String::from_utf8_lossy(&bytes);
                println!("{address} -> {text}\r");
                eprintln!("{address}: line exceeded {MAX_LINE_BYTES} bytes; closing connection");
                break;
            }
            Ok(ReadLine::Closed) => break,
            Err(error) => {
                eprintln!("{address}: error reading from client: {error}");
                break;
            }
        }
    }

    remove_client(&mut username, &room);
}

fn read_protocol_line<R: BufRead>(reader: &mut R) -> io::Result<ReadLine> {
    let mut line = Vec::new();

    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(ReadLine::Closed);
        }

        if line.last() == Some(&b'\r') && available[0] == b'\n' {
            line.push(b'\n');
            reader.consume(1);
            return Ok(ReadLine::Complete(line));
        }

        if let Some(position) = available.windows(2).position(|bytes| bytes == b"\r\n") {
            let end = position + 2;
            if line.len() + end <= MAX_LINE_BYTES {
                line.extend_from_slice(&available[..end]);
                reader.consume(end);
                return Ok(ReadLine::Complete(line));
            }
        }

        let remaining = MAX_LINE_BYTES - line.len();
        let consumed = available.len().min(remaining);
        line.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if line.len() == MAX_LINE_BYTES {
            return Ok(ReadLine::TooLong(line));
        }
    }
}

fn parse_command(line: &str) -> Command<'_> {
    if line == "EXIT" {
        return Command::Exit;
    }
    if line == "JOIN" {
        return Command::Join("");
    }
    if let Some(username) = line.strip_prefix("JOIN ") {
        return Command::Join(username);
    }
    if line == "PUBLIC" {
        return Command::Public("");
    }
    if let Some(message) = line.strip_prefix("PUBLIC ") {
        return Command::Public(message);
    }
    if line == "PRIVATE" {
        return Command::Private {
            recipient: "",
            message: "",
        };
    }
    if let Some(arguments) = line.strip_prefix("PRIVATE ") {
        return match arguments.split_once(' ') {
            Some((recipient, message)) => Command::Private { recipient, message },
            None => Command::Private {
                recipient: arguments,
                message: "",
            },
        };
    }

    Command::Invalid
}

fn handle_command(
    line: &str,
    username: &mut Option<String>,
    writer: &SharedWriter,
    room: &Arc<Mutex<Room>>,
) -> bool {
    let mut room = room.lock().unwrap();

    match parse_command(line) {
        Command::Join(requested_username) => {
            if username.is_some() {
                return send_status(writer, 500);
            }
            if requested_username.is_empty() {
                return send_status(writer, 104);
            }
            if requested_username.chars().count() > 16 {
                return send_status(writer, 102);
            }
            if !valid_username(requested_username) {
                return send_status(writer, 103);
            }
            if room.members.contains_key(requested_username) {
                return send_status(writer, 101);
            }

            let requested_username = requested_username.to_owned();
            room.members
                .insert(requested_username.clone(), Arc::clone(writer));
            *username = Some(requested_username.clone());
            print_members(&room);
            broadcast(&room, format!("JOINED {requested_username}\r\n").as_bytes());
            send_status(writer, 100)
        }
        Command::Exit => {
            if let Some(leaving_username) = username.take() {
                room.members.remove(&leaving_username);
                broadcast(&room, format!("LEFT {leaving_username}\r\n").as_bytes());
                print_members(&room);
            }
            send_status(writer, 200);
            false
        }
        Command::Public(message) => {
            let Some(sender) = username.as_deref() else {
                return send_status(writer, 500);
            };
            if message.contains('\r') || message.contains('\n') {
                return send_status(writer, 500);
            }
            if message.chars().count() > 1000 {
                return send_status(writer, 301);
            }

            broadcast(&room, format!("PUBLIC {sender} {message}\r\n").as_bytes());
            send_status(writer, 300)
        }
        Command::Private { recipient, message } => {
            let Some(sender) = username.as_deref() else {
                return send_status(writer, 500);
            };
            if !valid_username(recipient) {
                return send_status(writer, 403);
            }
            if message.contains('\r') || message.contains('\n') {
                return send_status(writer, 500);
            }
            if message.chars().count() > 1000 {
                return send_status(writer, 402);
            }
            let Some(recipient_writer) = room.members.get(recipient) else {
                return send_status(writer, 401);
            };

            write_line(
                recipient_writer,
                format!("PRIVATE {sender} {message}\r\n").as_bytes(),
            );
            send_status(writer, 400)
        }
        Command::Invalid => send_status(writer, 500),
    }
}

fn valid_username(username: &str) -> bool {
    !username.is_empty()
        && username.len() <= 16
        && username.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn send_status(writer: &SharedWriter, status: u16) -> bool {
    write_line(writer, format!("{status:03}\r\n").as_bytes())
}

fn write_line(writer: &SharedWriter, line: &[u8]) -> bool {
    match writer.lock() {
        Ok(mut stream) => match stream.write_all(line) {
            Ok(()) => true,
            Err(error) => {
                eprintln!("Could not write to client: {error}");
                false
            }
        },
        Err(error) => {
            eprintln!("Client writer lock was poisoned: {error}");
            false
        }
    }
}

fn broadcast(room: &Room, line: &[u8]) {
    for recipient in room.members.values() {
        write_line(recipient, line);
    }
}

fn print_members(room: &Room) {
    let mut usernames: Vec<_> = room.members.keys().cloned().collect();
    usernames.sort();
    println!("Room members: {usernames:?}");
}

fn remove_client(username: &mut Option<String>, room: &Arc<Mutex<Room>>) {
    let Some(username) = username.take() else {
        return;
    };
    let mut room = room.lock().unwrap();
    room.members.remove(&username);
    broadcast(&room, format!("LEFT {username}\r\n").as_bytes());
    print_members(&room);
}

#[cfg(test)]
mod tests {
    use super::{
        Command, MAX_LINE_BYTES, ReadLine, parse_command, read_protocol_line, valid_username,
    };
    use std::io::Cursor;

    #[test]
    fn parses_commands_and_preserves_message_spaces() {
        assert!(matches!(parse_command("JOIN "), Command::Join("")));
        assert!(matches!(
            parse_command("PUBLIC hello  there"),
            Command::Public("hello  there")
        ));
        assert!(matches!(
            parse_command("PRIVATE Ada hello  there"),
            Command::Private {
                recipient: "Ada",
                message: "hello  there"
            }
        ));
        assert!(matches!(
            parse_command("PRIVATE Ada"),
            Command::Private {
                recipient: "Ada",
                message: ""
            }
        ));
        assert!(matches!(parse_command("EXIT extra"), Command::Invalid));
    }

    #[test]
    fn username_validation_accepts_only_one_to_sixteen_ascii_letters_or_digits() {
        assert!(valid_username("Ada7"));
        assert!(!valid_username(""));
        assert!(!valid_username("Ada_7"));
        assert!(!valid_username("abcdefghijklmnopq"));
        assert!(!valid_username("Åda"));
    }

    #[test]
    fn reads_crlf_lines_without_treating_bare_line_feeds_as_delimiters() {
        let mut reader = Cursor::new(b"PUBLIC a\nb\r\nEXIT\r\n");

        assert!(matches!(
            read_protocol_line(&mut reader).unwrap(),
            ReadLine::Complete(line) if line == b"PUBLIC a\nb\r\n"
        ));
        assert!(matches!(
            read_protocol_line(&mut reader).unwrap(),
            ReadLine::Complete(line) if line == b"EXIT\r\n"
        ));
    }

    #[test]
    fn enforces_the_line_limit_including_the_crlf_terminator() {
        let mut legal_line = vec![b'x'; MAX_LINE_BYTES - 2];
        legal_line.extend_from_slice(b"\r\n");
        let mut reader = Cursor::new(legal_line.clone());
        assert!(matches!(
            read_protocol_line(&mut reader).unwrap(),
            ReadLine::Complete(line) if line.len() == MAX_LINE_BYTES
        ));

        let mut reader = Cursor::new(vec![b'x'; MAX_LINE_BYTES]);
        assert!(matches!(
            read_protocol_line(&mut reader).unwrap(),
            ReadLine::TooLong(line) if line.len() == MAX_LINE_BYTES
        ));
    }
}
