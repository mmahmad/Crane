import socket
import json
# create socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

sock.bind(('127.0.0.1', 9999))

while(1):
    task_details, addr = sock.recvfrom(10)
    print json.loads(task_details)