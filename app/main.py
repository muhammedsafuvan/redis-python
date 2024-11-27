import os
import socket
import threading  # noqa: F401
import time
import sys
from datetime import datetime
from typing import BinaryIO
import argparse
from io import BytesIO

parser = argparse.ArgumentParser(
    prog="Bootleg Redis",
    description="'Build your own Redis' Codecrafters challenge",
)
parser.add_argument("--dir")
parser.add_argument("--dbfilename")
parser.add_argument("--port", type=int, default=6379)
parser.add_argument("--replicaof")
args = parser.parse_args()
dir_path = b""
db_filename = b""
key_values = dict()

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
            print(f"COMMAND {command}")
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
                rdb_content = get_rdb()
                
                if command[1] in expiry_dict and expiry_dict[command[1]] < time.time():
                    del set_dict[command[1]]  
                    del expiry_dict[command[1]]
                if rdb_content:
                    key_values = parse_redis_file_format(rdb_content)
                    print(f"KEY VALS {key_values}")
                    
                    value = key_values.get(command[1].decode('utf-8')).get('value')
                    expiry_time = key_values.get(command[1].decode('utf-8')).get('expiry_time')
                    
                    
                    if time.time() > expiry_time/1000:
                        key = command[1].decode('utf-8')  # Decode the key from the command
                        if key in key_values:
                            del key_values[key]  
                        connection.send(response)
                    else:
                        response = b"$" + str(len(value)).encode() + b"\r\n" + value.encode() + b"\r\n"
                        connection.send(response)
                elif command[1] in set_dict.keys():
                    message = set_dict[command[1]]
                    response = b"$" + str(len(message)).encode() + b"\r\n" + message + b"\r\n"
                    connection.send(response)
                else:
                    response = b"$-1\r\n"
                    connection.send(response)
            elif len(command) > 2 and command[0] == b"CONFIG" and command[1] == b"GET":
                if command[2] == b"dir":
                    response = b"*2\r\n$3\r\ndir\r\n$" + str(len(dir_path)).encode() + b"\r\n" + dir_path.encode() + b"\r\n"
                    connection.send(response)
                elif command[2] == b"dbfilename":
                    response = b"*2\r\n$9\r\ndbfilename\r\n$" + str(len(db_filename)).encode() + b"\r\n" + db_filename.encode() + b"\r\n"
                    connection.send(response)

            elif len(command) > 1 and command[0] == b"KEYS":
                if command[1] == b"*": 
                    rdb_content = get_rdb()
                    print(f"RDB CONTENT {rdb_content}")
                    if rdb_content is not None:
                        # keys = multiple_keys(rdb_content)
                        key_values = parse_redis_file_format(rdb_content)
                        print(f"KEY VALS {key_values}")
                        res_list = [f"${len(key)}\r\n{key}\r\n" for key, val in key_values.items()]
                        print(f"RES LIST {res_list}")
                        response = f"*{len(key_values)}\r\n".encode() + "".join(res_list).encode()

                        print(f"RES {response}")
                        connection.send(response)
                else:
                    response = "*0\r\n".encode()
                    connection.send(response)

def parse_redis_file_format(file_format):
    splited_parts = str(file_format).split("\\")
    print(f"SPLIT PARTS {splited_parts}")
    resizedb_index = splited_parts.index("xfb")
    index = resizedb_index + 3
    while index < len(splited_parts):
        expiry = None
        if splited_parts[index].startswith("xff"):
            break
        if splited_parts[index] == "xfc":
            f = BytesIO(file_format)  # Convert bytes to a file-like object
            expiry, f = rdb_file_process_expiry(f, 8)
            print(f"EXPIRY {expiry}")

            index += 9

        key = remove_bytes_characteres(splited_parts[index])
        val = remove_bytes_characteres(splited_parts[index+1])
        if key:
            key_values[key] = {'value': val, 'expiry_time': expiry}
        
        
        
        index += 2

    return key_values

def rdb_file_process_expiry(f: BinaryIO, bytes_to_read: int) -> tuple[float, BinaryIO]:
    if bytes_to_read == 4:
        expire_seconds = int.from_bytes(f.read(bytes_to_read), byteorder="little")
        return (expire_seconds, f)
    elif bytes_to_read == 8:
        expire_ms = int.from_bytes(f.read(bytes_to_read), byteorder="little") / 1000
        return (expire_ms, f)
    else:
        raise ValueError("Unable to process expiry time for key_value read from file!")

def clean_hex(data):
    cleaned_data = []
    for item in data:
        try:
            # Remove 'x' prefix and convert to an integer (base 16)
            cleaned_item = item.replace('x', '')  # Remove the 'x' prefix
            cleaned_item = cleaned_item.strip('~')  # Remove the '~' character if it exists
            
            # Convert the cleaned hexadecimal string to an integer (base 16)
            cleaned_data.append(int(cleaned_item, 16))

        except ValueError:
            # Skip invalid entries
            continue
    return cleaned_data

def get_rdb():
    rdb_file_path = os.path.join(dir_path, db_filename)
    if os.path.exists(rdb_file_path):
        with open(rdb_file_path, "rb") as rdb_file:
            rdb_content = rdb_file.read()
            return rdb_content
        
def remove_bytes_characteres(string: str):
    if string.startswith("x"):
        return string[3:]
    elif string.startswith("t"):
        return string[1:]
    elif string.startswith("n") and len(string)>2:
        return string[1:]
    


def main():
    """
    Main server loop.
    """
    global dir_path, db_filename

    print("Logs from your program will appear here!")
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i] == '--dir':
            dir_path = sys.argv[i + 1]
        elif sys.argv[i] == '--dbfilename':
            db_filename = sys.argv[i + 1]


    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(connection,)).start()


if __name__ == "__main__":
    main()
