import socket
import threading  # noqa: F401
import time

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
    set_dict = dict()
    expiry_dict = dict()
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
            elif command[0] == b"SET":
                if len(command) > 4 and command[3].upper() == b"PX":
                    set_dict[command[1]] = command[2]
                    expiry_time = time.time() + (int(command[4])/1000)
                    print(expiry_time)
                    expiry_dict[command[1]] = expiry_time

                elif len(command) > 2:
                    set_dict[command[1]] = command[2]

                connection.send(b"+OK\r\n")
            elif len(command) > 1 and command[0] ==b"GET":

                if command[1] in expiry_dict and expiry_dict[command[1]] < time.time():
                    del set_dict[command[1]]  
                    del expiry_dict[command[1]]
                if command[1] in set_dict.keys():
                    message = set_dict[command[1]]
                    response = b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"
                    connection.send(response)
                else:
                    response = b"$-1\r\n"
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
