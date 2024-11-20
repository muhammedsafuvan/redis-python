import socket
import threading  # noqa: F401


def protocol_parser(data):
    """
    Parse RESP protocol data into a list of commands.
    """
    commands = []
    parts = data.split(b"\r\n")
    i = 0
    while i < len(parts):
        if parts[i].startswith(b'*'):  # Array
            num_elements = int(parts[i][1:])
            i += 1
            command = []
            for _ in range(num_elements):
                if i < len(parts) and parts[i].startswith(b'$'):  # Bulk string
                    length = int(parts[i][1:])
                    i += 1
                    if i < len(parts) and len(parts[i]) == length:
                        command.append(parts[i])
                    i += 1
            commands.append(command)
        else:
            i += 1
    return commands


def handle_client(connection):
    """
    Handle a single client connection.
    """
    buffer = b""
    while True:
        data = connection.recv(8000)
        if not data:
            break

        buffer += data
        if b"\r\n" not in buffer:
            continue

        commands = protocol_parser(buffer)
        buffer = b""  # Reset buffer after parsing

        for command in commands:
            if len(command) > 0 and command[0] == b"PING":
                connection.send(b"+PONG\r\n")
            elif len(command) > 1 and command[0] == b"ECHO":
                message = command[1]
                response = b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"
                connection.send(response)


def main():
    """
    Main server loop.
    """
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(connection,)).start()


if __name__ == "__main__":
    main()
