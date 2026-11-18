"""Quiz console client, PHASE 1: connect, JOIN, then send test messages.

Usage: python client.py [server_ip] [port]
Type any text to send it as an ANSWER (the server rejects it in phase 1),
type 'quit' to leave.
"""

import socket
import sys

import protocol as p


def show(message: dict) -> None:
    """Display one server message in a readable way."""
    msg_type = message["type"]
    if msg_type == p.WELCOME:
        print(f"[server] Welcome, you are player {message.get('player_id')}")
    elif msg_type == p.LOBBY:
        print(f"[server] Players in the lobby: {', '.join(message.get('players', []))}")
    elif msg_type == p.ERROR:
        print(f"[server] Error: {message.get('reason')}")
    else:
        print(f"[server] {message}")


def main() -> None:
    address = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else p.PORT

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((address, port))
    except OSError as exc:
        print(f"Cannot connect to {address}:{port}: {exc}")
        return
    channel = p.MessageChannel(sock)
    print("Connected to the server")

    try:
        name = input("Your name: ").strip()
        channel.send_message(p.make_message(p.JOIN, name=name))

        # The server answers a JOIN with two messages (WELCOME then LOBBY),
        # unless the name is rejected (one ERROR message).
        first = channel.recv_message()
        if first is None:
            print("Server closed the connection")
            return
        show(first)
        if first["type"] == p.WELCOME:
            show(channel.recv_message())

        while True:
            text = input("> ").strip()
            if text.lower() == "quit":
                channel.send_message(p.make_message(p.QUIT))
                break
            channel.send_message(p.make_message(p.ANSWER, q=0, choice=text.upper()))
            reply = channel.recv_message()
            if reply is None:
                print("Server closed the connection")
                break
            show(reply)
    except (p.ProtocolError, OSError) as exc:
        print(f"Connection problem: {exc}")
    except (EOFError, KeyboardInterrupt):
        print("\nBye")
    finally:
        channel.close()
        print("Connection closed")


if __name__ == "__main__":
    main()