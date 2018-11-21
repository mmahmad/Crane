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
import pprint
import collections
import node
import random

def get_process_hostname():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	return s.getsockname()[0]	
	s.close()

NIMBUS_LISTEN_PORT = 20000
NIMBUS_IP = get_process_hostname()
SUPERVISOR_LISTEN_PORT = 6000
SPOUT_LISTEN_PORT = 4999

class Nimbus(object):
	def __init__(self):
		self.list_of_workers = []
		self.host = NIMBUS_IP
		self.port = NIMBUS_LISTEN_PORT
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind((self.host, self.port))
		self.worker_mapping = collections.defaultdict(list)
		self.reverse_mapping = {}
		self.machine_list = []

		t1 = threading.Thread(target = self.listen, args = ())
		t1.daemon = True
		t1.start()

		# start node failure detection component
		failure_detector_node = node.Node()
		failure_detector_node.start()

	def listen(self):
		print "Waiting for worker to connect..."

		while (1):
			data, addr = self.sock.recvfrom(1024)
			print 'Connected with:' + str(addr[0]) + ':' + str(addr[1])
			
			data = json.loads(data)

			if data['type'] == 'START_JOB':
				self.config = data['config']
				self.assign_jobs()
			elif data['type'] == 'JOIN_WORKER':
				self.machine_list.append(addr[0])
			elif data['type'] == 'FAIL':
				self.reassign_jobs(data['failed_node'])
	
	def reassign_jobs(self, failed_node):
		failed_node_ip = failed_node[0]
		jobs_to_reassign = self.worker_mapping[failed_node_ip]
		
		#Remove IP from list of alive machines
		self.machine_list.remove(failed_node_ip)
		#Remove IP from IP->job mapping
		del self.worker_mapping[failed_node_ip]

		for job in jobs_to_reassign:
			new_node = random.choice(self.machine_list)

	def assign_jobs(self):
		port = 5000
		print 'List of alive machine IPs is'
		print self.machine_list

		spout_node = None
		counter = 0
		for worker in self.config:
			self.config[worker]['worker_id'] = worker
			if self.config[worker]['type'] == 'spout':
				spout_node = worker
			self.worker_mapping[self.machine_list[counter]].append(worker)

			if self.config[worker]['type'] == 'bolt':
				self.reverse_mapping[worker] = (self.machine_list[counter], port)
				port += 1
			else:
				self.reverse_mapping[worker] = self.machine_list[counter]
			counter = (counter + 1) % (len(self.machine_list))
		
		print 'Job assignment:'
		print self.worker_mapping
		print 'Port mapping:'
		print self.reverse_mapping

		#Find spout IP to send to everybody so they can send ACKs to spout
		spout_ip = self.reverse_mapping[spout_node]

		for worker in self.config:
			self.config[worker]['spout_ip_port'] = (spout_ip, SPOUT_LISTEN_PORT)
			if self.config[worker]['type'] == 'bolt':
				self.config[worker]['listen_port'] = self.reverse_mapping[worker][1]
			try:
				children =self.config[worker]['children']
				self.config[worker]['children_ip_port'] = []
				for child in children:
					self.config[worker]['children_ip_port'].append(self.reverse_mapping[child])
			except KeyError as e:
				pass

			try:
				parents = self.config[worker]['parents']
				self.config[worker]['parent_ip_port'] = []
				for parent in parents:
					self.config[worker]['parent_ip_port'].append(self.reverse_mapping[parent])
			except KeyError as e:
				pass

		print 'Updated config with parent and children IPs'
		pprint.pprint(self.config)

		for machine in self.machine_list:
			for worker in self.worker_mapping[machine]:
				try:
					sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
					sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
					sock.sendto(json.dumps({'type': 'NEW', 'task_details':self.config[worker]}), (machine, SUPERVISOR_LISTEN_PORT))
				except:
					print 'Unable to contact worker'
					return
			
def main():
	nimbus = Nimbus()
	# nimbus.listen()
	
if __name__ == '__main__':
	main()