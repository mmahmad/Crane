import socket
import threading
import logging
import json
import random
import time
import sys
import subprocess
import os
import yaml
import Queue
import time

def get_process_hostname():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	return s.getsockname()[0]	
	s.close()

NIMBUS_LISTEN_PORT = 20000
NIMBUS_IP = get_process_hostname()

class Nimbus(object):
	def __init__(self):
		self.list_of_workers = []
		self.host = NIMBUS_IP
		self.port = NIMBUS_LISTEN_PORT
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind((self.host, self.port))
	
	def listen(self):
		print "Waiting for worker to connect..."

		while (1):
			data, addr = self.sock.recvfrom(1024)
			print 'Connected with:' + str(addr[0]) + ':' + str(addr[1])
			
			data = json.loads(data)

			if data['type'] == 'START_JOB':
				self.config = data['config']
				print self.config
			elif data['type'] == 'JOIN_WORKER':
				self.list_of_workers.append(data['ip'])

def main():
	nimbus = Nimbus()
	nimbus.listen()

if __name__ == '__main__':
	main()