import socket
import threading
from thread import *
import json
import os.path
from datetime import datetime
################################
def get_process_hostname():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	return s.getsockname()[0]	
	s.close()

class Introducer(object):
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind((self.host, self.port))
		self.master = None
		self.file_list = None
		self.lock = threading.Lock()


		if not os.path.exists('membership.json'):
			with open('membership_list.json', 'w+') as mem_list:
				self.membership_list = {"members" : []}
				json.dump(self.membership_list, mem_list)

		with open('membership_list.json') as mem_list:
			self.membership_list = json.load(mem_list)

	def listen(self):
		print "Waiting for client to connect..."

		while (1):
			data, addr = self.sock.recvfrom(1024)
			print 'Connected with:' + str(addr[0]) + ':' + str(addr[1])	

			t = threading.Thread(target = self.threaded, args = (data, addr))
			t.start() 

	def threaded(self, data, addr): 
		data = json.loads(data)

		if data['type'] == 'FAIL' or data['type'] == 'LEAVE':
			if data['type'] == 'FAIL':
				remove_node = tuple(data['failed_node'])
			else:
				remove_node = tuple(data['left_node'])

			if self.master == remove_node:
				# ping master to confirm its status. If no ack, remove from membershiplist, re-elect new master, send file_list to it, update all other nodes

				data = {
					'type': 'INTRODUCER_MASTER_STATUS_VALIDATION'
				}
				sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sock.sendto(json.dumps(data), (self.master[0], 50000))

				sock.settimeout(0.8)

				try:
					message, address = sock.recvfrom(1024)
					if message == 'ACK':
						return
				except socket.timeout as e:
					new_membership_list = [member for member in self.membership_list['members'] if member['id'] != remove_node]
					
					self.lock.acquire()
					old_master = self.master
					self.master = new_membership_list[-1]['id']

					if old_master == self.master:
						#Duplicate failure message detected
						self.lock.release()
						return
					
					self.lock.release()

					print 'Chose master node: '
					print self.master

					data = {
						'type': 'UPDT_MASTER',
						'master': self.master,
						'previous_master': old_master,
						'file_list': self.file_list
					}

					for member in self.membership_list['members']:
						addr = (member['id'][0], 50000)
						self.sock.sendto(json.dumps(data), addr)
						print 'Sent newly elected master ID to' + str(addr[0])	
			else:
				new_membership_list = [member for member in self.membership_list['members'] if member['id'] != remove_node]

			self.membership_list['members'] = new_membership_list

			#Make changes in file as well
			with open('membership_list.json', 'w+') as mem_list:
				json.dump(self.membership_list, mem_list)

		elif data['type'] == 'JOIN': 
			new_ip = addr[0]
			
			for member in self.membership_list['members']:
				if new_ip == member['id'][0]:
					return

			new_member = {
				'id': (new_ip, str(datetime.now()))
			}

			if self.master is None:
				self.master = new_member['id']
				print self.master

			self.membership_list['members'].append(new_member)

			try:
				data = {
					'type': 'LIST',
					'members': self.membership_list['members'],
					'master': self.master
				}
				self.sock.sendto(json.dumps(data), addr)
			except:
				print 'Error occured'
			
			data = {
				'type': 'UPDT',
				'members': self.membership_list['members']
			}

			for member in self.membership_list['members'][:-1]:
				addr = (member['id'][0], 50000)
				self.sock.sendto(json.dumps(data), addr)
				print 'Sent updated membership list to' + str(addr[0])	

			with open('membership_list.json', 'w+') as mem_list:
				json.dump(self.membership_list, mem_list)

		elif data['type'] == 'FILE_LIST':
			self.file_list = data['file_list']
			
if __name__ == '__main__':
	PROCESS_INIT_PORT, PROCESS_HOSTNAME = 45001, get_process_hostname()
	Introducer(PROCESS_HOSTNAME, PROCESS_INIT_PORT).listen()
