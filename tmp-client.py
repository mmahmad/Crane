import socket
import json

IP = '127.0.0.1'
PORT = 9999

ack_message = 0

while True:
    ack_message += 1
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.sendto(json.dumps(ack_message), (IP, PORT))