import socket
from thread import *
import threading
import logging
import json
import random
import time
import sys
import subprocess
import os
import Queue
import time

logging.basicConfig(
	level = logging.DEBUG,
	format='%(asctime)s %(levelname)s %(message)s',
	filename='mp2.log',
	filemode='w+',
)

LOGGER = logging.getLogger('MP2')

def get_process_hostname():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.connect(("8.8.8.8", 80))
	return s.getsockname()[0]
	s.close()


NODE_IP = get_process_hostname()  # this node's IP
NODE_RECVPORT = 50000
FILE_SYSTEM_RECVPORT = 10000
FNULL = open(os.devnull, 'w')

INTRODUCER_IP = "fa18-cs425-g03-01.cs.illinois.edu" # VM1 is the introducer
INTRODUCER_PORT = 45001

class Node(object):
	def __init__(self):
		self.membership_list = []
		self.failed_nodes = [] # List of nodes that we did not receive ACKs from
		self.lock = threading.Lock()
		self.command = None
		self.is_master = False
		self.master_id = None
		self.queue = Queue.Queue()
		
	def introduce(self):
		# We are a new node, so need to contact introducer

		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			data = {"type": "JOIN", "members": []}
			sock.sendto(json.dumps(data), (INTRODUCER_IP, INTRODUCER_PORT))

			result = sock.recv(1024000)

			# remove myself from membership list
			new_membership_list = json.loads(result)['members']
			self.id = [member for member in new_membership_list if member['id'][0] == NODE_IP][0]['id']
			new_membership_list = [member for member in new_membership_list if member['id'][0] != NODE_IP]

			# shuffle the list so that all nodes interact with different nodes from the beginning
			random.shuffle(new_membership_list)

			self.lock.acquire()
			self.membership_list = new_membership_list
			self.master_id = json.loads(result)['master']
			self.lock.release()

			#If introducer elects this node as the master, set the is_master flag to True
			#Can only be set to True for any one node at a given point of time
			if json.loads(result)['master'] == self.id:
				self.is_master = True
				self.file_list = {}

			file_list = os.listdir('sdfs_files')

			# on joining, clean previous node's sdfs_files
			for file in file_list:
				os.remove('sdfs_files/' + file)

		except socket.error as e:  # If connection to remote machine fails
			print 'INTRODUCE(): Could not connect to the introducer at ' + str(INTRODUCER_IP)
			LOGGER.info('INTRODUCE(): Could not connect to the introducer at %s', str(INTRODUCER_IP))
			return

	def client(self):
		counter = 0  # points to membership list index of last node to whom ping was sent
		thread_list = []

		while not self.command:
			pass

		while self.command == "JOIN":
			## get 4 members from the membership list, using counter to know which ones
			num_members = len(self.membership_list)

			if num_members < 4:
				pass
			else:
				ping_targets = [self.membership_list[counter%num_members], self.membership_list[(counter + 1)%num_members], self.membership_list[(counter + 2)%num_members], self.membership_list[(counter + 3)%num_members]]
				new_counter = (counter+4)%num_members

				if new_counter <= counter:
					counter = 0
					random.shuffle(self.membership_list)
				else:
					counter = new_counter

				for ping_target in ping_targets:
					addr = (ping_target['id'][0], NODE_RECVPORT)
					t_id = threading.Thread(target = self.threadPingAndWait,args =  (addr, ping_target['id']))
					t_id.start()
					thread_list.append(t_id)

				for thread in thread_list:
					thread.join()

		if self.command == "LEAVE":
			# tell everyone that we're leaving so that they can remove us from the membership list
			data = {
				'type': 'LEAVE',
				'left_node': self.id
			}

			LOGGER.info('client(): Node %s is leaving the system ', str(self.id))

			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			
			for member in self.membership_list:
				addr = (member['id'][0], NODE_RECVPORT)
				try:
					sock.sendto(json.dumps(data), addr)
				except socket.error as e:
					LOGGER.info('client(): unable to connect to member %s to disseminate LEAVE', member)

			# connect to the introducer and send the left node's information to it

			data = {
				'type': 'LEAVE',
				'left_node': self.id
			}

			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sock.sendto(json.dumps(data), (INTRODUCER_IP, INTRODUCER_PORT))

			except socket.error as e:  # If connection to the introducer fails
				print 'client(): Could not connect to the introducer at ' + str(INTRODUCER_IP)
				LOGGER.info('client(): Could not connect to the introducer at %s', str(INTRODUCER_IP))
				return

	def threadPingAndWait(self, addr, id):
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		data = {
			'type': 'PING',
			'failed_nodes': self.failed_nodes
		}
		
		sock.sendto(json.dumps(data), addr)
		sock.settimeout(0.8)

		try:
			message, address = sock.recvfrom(1024)
			if message == 'ACK':
				return
		except socket.timeout as e:
			sock.sendto(json.dumps(data), addr)
			sock.settimeout(0.6)
			# try again for failed node
			try:
				message, address = sock.recvfrom(1024)
				if message == 'ACK':
					return
			except socket.timeout:
				# if failed, add
				self.lock.acquire()

				# connect to the introducer and send the failed node's information to it

				data = {
					'type': 'FAIL',
					'failed_node': id
				}
				print "Detected node failure"
				print data

				try:
					sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
					sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
					sock.sendto(json.dumps(data), (INTRODUCER_IP, INTRODUCER_PORT))

				except socket.error as e:  # If connection to the introducer fails
					print 'threadPingAndWait(): Could not connect to the introducer at ' + str(INTRODUCER_IP)
					LOGGER.info('threadPingAndWait(): Could not connect to the introducer at %s', str(INTRODUCER_IP))
					return
				
				# Add the node's id to our own list if it's not already there
				if id not in self.failed_nodes:
					self.failed_nodes.append(id)
					LOGGER.info('CLIENT: Found new failure node %s',(id))

				start_time = time.time()
				self.membership_list = [member for member in self.membership_list if member['id'] != id]
				
				# if master node detects a failure, needs to find all the files that were stored on that node.
				# re-replicate those files on other nodes.
				# Then, update file_list
				# then, send update to the introducer (in case master fails)

				if self.is_master:
					files_to_replicate = []
					for file_name in self.file_list:
						if {'id': id} in self.file_list[file_name][0]:
							self.file_list[file_name][0].remove({'id':id})
							files_to_replicate.append(file_name)

					thread_list = []
					for file_name in files_to_replicate:
						possible_new_replica_list = [node for node in self.membership_list if node not in self.file_list[file_name][0]]
						
						to_replica = random.choice(possible_new_replica_list)
						from_replica = self.file_list[file_name][0][0]['id'][0]

						rt = threading.Thread(target=self.transfer_replica, args = (from_replica, file_name, to_replica))
						rt.daemon = True
						thread_list.append(rt)
						rt.start()

						self.file_list[file_name][0].append(to_replica)

					for thread_id in thread_list:
						thread_id.join()

					# send updated file_list to introducer
					sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
					sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
					sock.sendto(json.dumps({'type': 'FILE_LIST', 'file_list': self.file_list}), (INTRODUCER_IP, INTRODUCER_PORT))
					
					end_time = time.time()
					print 'Time to replicate file:'
					print end_time - start_time

				self.lock.release()
				return
	
	def transfer_replica(self, from_replica, file_name, to_replica):
		file_name = file_name + '-v0'
		scp_command = 'scp mmahmad3@{source}:~/cs425mp3/sdfs_files/{source_file_name} mmahmad3@{replica_node}:~/cs425mp3/sdfs_files/{sdfs_file_name}'.format(source = from_replica, source_file_name = file_name, replica_node = to_replica['id'][0], sdfs_file_name = file_name)

		try:
			retData = subprocess.check_call(scp_command, shell=True)
		except:
			return

	# Open connection for other nodes to connect to so that they can ping us and send messages
	def server(self):
	
		# declare our serverSocket upon which
		# we will be listening for UDP messages
		serversocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		# One difference is that we will have to bind our declared IP address
		# and port number to our newly declared serverSock
		serversocket.bind((NODE_IP, NODE_RECVPORT))

		# waiting for nodes to connect. Hand it off to a new thread once conn. established with a node
		while not self.command:
			pass

		print "Command: ", self.command
		while self.command == "JOIN":
			try:
				data, addr = serversocket.recvfrom(102400)
			except:
				print "server(): Message not received"

			# Start a new thread and return its identifier
			t = threading.Thread(target = self.threadRecvConn, args = (data, addr))
			t.start()

		return

	# thread fuction 
	# can receive message from introducer, or a PING request, or an ACK
	def threadRecvConn(self, data, addr): 
  
		# received ping + failed node(s) list
		receivedInfo = json.loads(data) # convert into json format
		receivedIP = addr[0]
		receivedPort = addr[1]

		# send ACK (if PING request), or do nothing if ack received, or update membership list if UPDT received

		# Sending ACK to PING request
		if (receivedInfo['type'] == 'PING'):
			try:
	
				sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sock.sendto('ACK', (receivedIP, receivedPort))
				
				# append failed_nodes to my failed nodes
				
				if len(receivedInfo['failed_nodes']) > 0:
					self.lock.acquire()
					failed_nodes = receivedInfo['failed_nodes']

					for node in failed_nodes:
						if node not in self.failed_nodes:
							self.failed_nodes.append(node)
						
					# remove those failed_nodes from my membership_list
					new_membership_list = [member for member in self.membership_list if member['id'] not in self.failed_nodes]

					if len(new_membership_list) == len(self.membership_list):
						pass
					else:
						LOGGER.info('SERVER: Received new failure information. Membership list modified due to new node failure info.')
						self.membership_list = new_membership_list

					self.lock.release()

				return
			except socket.error as e: #If connection to remote machine fails
				print 'PING: Could not connect to ' + str(INTRODUCER_IP)
				return

		# if introducer gets to know of master failure, it can ping us (if we were the master) to confirm that it was not a false +ve
		# 
		elif receivedInfo['type'] == 'INTRODUCER_MASTER_STATUS_VALIDATION':
			if self.is_master: # not really needed, but just in code is too fragile
				sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sock.sendto('ACK', (INTRODUCER_IP, INTRODUCER_PORT))
				

		elif (receivedInfo['type'] == 'UPDT'):
			
			new_membership_list = receivedInfo['members']
			
			# remove myself from membership list
			new_membership_list = [member for member in new_membership_list if member['id'][0] != NODE_IP]

			# update our membership list
			self.lock.acquire()

			# find out which node is added
			new_node = [x for x in new_membership_list if x not in self.membership_list]
			if new_node:
				new_node = new_node[0]
				LOGGER.info('threadRecvConn(): New node %s joined system', (str(new_node)))
			self.membership_list = new_membership_list
			self.lock.release()

			return

		elif receivedInfo['type'] == "LEAVE":
			# remove member from my membership list
			leaveId = receivedInfo['left_node']

			self.lock.acquire() 
			self.membership_list = [member for member in self.membership_list if member['id'] != leaveId]
			LOGGER.info('threadRecvConn(): Node %s is leaving the system', str(leaveId))					
			self.lock.release()
			
			return

		elif receivedInfo['type'] == 'UPDT_MASTER':
			new_master = receivedInfo['master']

			self.lock.acquire()
			self.master_id = new_master

			if new_master == self.id:
				self.is_master = True
				
				previous_master = receivedInfo['previous_master']
				print "prev. master was: "
				print previous_master
				self.file_list = receivedInfo['file_list']
				
				# replicate previous master's files (should also update the entry)
				files_to_replicate = []
				for file_name in self.file_list:
					if {'id': previous_master} in self.file_list[file_name][0]:
						self.file_list[file_name][0].remove({'id':previous_master})
						files_to_replicate.append(file_name)

				for file_name in files_to_replicate:
					possible_new_replica_list = [node for node in self.membership_list if node not in self.file_list[file_name][0]]
					
					to_replica = random.choice(possible_new_replica_list)
					from_replica = self.file_list[file_name][0][0]['id'][0]

					rt = threading.Thread(target=self.transfer_replica, args = (from_replica, file_name, to_replica))
					rt.daemon = True
					rt.start()

					self.file_list[file_name][0].append(to_replica)

				# send updated file_list to introducer
				sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sock.sendto(json.dumps({'type': 'FILE_LIST', 'file_list': self.file_list}), (INTRODUCER_IP, INTRODUCER_PORT))
			self.lock.release()

	#Multithreaded TCP server to handle file system operations
	def fileSystemServer(self):
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind((NODE_IP, FILE_SYSTEM_RECVPORT))

		sock.listen(5)

		while True:
			conn, address = sock.accept()
			t_id = threading.Thread(target = self.fileSystemServerThread, args = (conn, address))
			t_id.daemon = True
			t_id.start()

	def fileSystemServerThread(self, conn, address):
		host, port = address[0], address[1]
		full_command = conn.recv(1024).strip()

		command = full_command.split()

		if command[0].upper() == 'GET' or command[0].upper() == 'G':
			sdfs_file_name, local_file_name = command[1], command[2]
			
			if self.is_master:
				# Always pick 1st replica

				try:
					replica = self.file_list[sdfs_file_name]
				except KeyError as e:
					conn.sendall('No such file exists')
					conn.close()
					return

				replica = replica[0][0]
				num_replicas = self.file_list[sdfs_file_name][1]
				sdfs_file_name += '-v' + str(num_replicas)

				
				# create SCP command (scp <source> <dest>)
				scp_command = 'scp mmahmad3@{replica_source}:~/cs425mp3/sdfs_files/{file_name_on_replica} mmahmad3@{requester}:~/cs425mp3/local_files/{new_file_name}'.format(replica_source=replica['id'][0], file_name_on_replica=sdfs_file_name, requester=host, new_file_name=local_file_name)

				# execute SCP
				try:
					retData = subprocess.check_call(scp_command, stdout = FNULL, shell=True)
				except:
					print 'unable to GET file'
					conn.close()
					return

				if retData == 0:
					conn.sendall('ACK')

		elif command[0].upper() == 'GET-VERSIONS' or command[0].upper() == 'GV':
			sdfs_file_name, num_versions, local_file_name = command[1], int(command[2]), command[3]

			if self.is_master:
				try:
					replica = self.file_list[sdfs_file_name]
				except KeyError as e:
					conn.sendall('No such file exists')
					conn.close()
					return

				replica = replica[0][0]
				latest_version = self.file_list[sdfs_file_name][1]

				transferred_versions = 0
				for version in range(latest_version, -1, -1):

					# create SCP command (scp <source> <dest>)
					sdfs_version = sdfs_file_name + '-v' + str(version)
					scp_command = 'scp mmahmad3@{replica_source}:~/cs425mp3/sdfs_files/{file_name_on_replica} mmahmad3@{requester}:~/cs425mp3/local_files/{new_file_name}'.format(replica_source=replica['id'][0], file_name_on_replica=sdfs_version, requester=host, new_file_name=sdfs_version)

					# execute SCP
					try:
						retData = subprocess.check_call(scp_command, stdout = FNULL, shell=True)
						print 'file transferred to local fs'
					except:
						print 'unable to GET file'
						conn.close()
						return

					transferred_versions += 1
					if transferred_versions == num_versions:
						break
				
				conn.sendall('All versions sent')

		elif command[0].upper() == 'PUT' or command[0].upper() == 'P':

			local_file_name, sdfs_file_name = command[1], command[2]
			thread_list = []

			if self.is_master:
				if sdfs_file_name in self.file_list:
					replicas = self.file_list[sdfs_file_name][0]
					num_replicas = self.file_list[sdfs_file_name][1]
					self.file_list[sdfs_file_name][1] += 1
					sdfs_file_name += '-v' + str(num_replicas + 1)
				else:
					replicas = random.sample(self.membership_list + [{'id':self.id}], 4)

					self.file_list[sdfs_file_name] = [replicas, 0]
					sdfs_file_name += '-v0'
				failures = 0

				for replica in replicas:
					# t_id = threading.Thread(target = self.threadHandleReplica, args = (replica, local_file_name, sdfs_file_name, host))
					# thread_list.append(t_id)
					# t_id.daemon = True
					# t_id.start()
					scp_command = 'scp mmahmad3@{source}:~/cs425mp3/local_files/{source_file_name} mmahmad3@{replica_node}:~/cs425mp3/sdfs_files/{sdfs_file_name}'.format(source = host, source_file_name = local_file_name, replica_node = replica['id'][0], sdfs_file_name = sdfs_file_name)

					try:
						retData = subprocess.check_call(scp_command, shell=True, stdout = FNULL, stderr = FNULL)
					except:
						self.queue.put(1)
				
				
				# for t in thread_list:
				# 	t.join()

				while not self.queue.empty():
					failures += self.queue.get()
				
				# print self.file_list
				if failures == 0:
					conn.sendall('ACK')
				else:
					#Pick <failures> new replicas
					pass

				# send updated file_list to introducer
				sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sock.sendto(json.dumps({'type': 'FILE_LIST', 'file_list': self.file_list}), (INTRODUCER_IP, INTRODUCER_PORT))


		elif command[0].upper() == 'LS' or command[0].upper() == 'L':
			sdfs_file_name = command[1]
			if self.is_master:
				try:
					replica_list = self.file_list[sdfs_file_name]
				except KeyError as e:
					conn.sendall(json.dumps(['No such file exists']))
					conn.close()
					return

				replica_list = replica_list[0]

				try:
					conn.sendall(json.dumps(replica_list))
				except socket.error as e:
					print 'Error occured while sending LS results'

		elif command[0].upper() == 'DELETE' or command[0].upper() == 'D':
			sdfs_file_name = command[1]

			if self.is_master:
				try:
					replicas = self.file_list[sdfs_file_name]
				except KeyError as e:
					conn.sendall('File does not exist')
					conn.close()
					return

				replicas = replicas[0]

				for replica in replicas:
					if replica['id'] == self.master_id:

						file_list = os.listdir('sdfs_files')

						for file in file_list:
							if file.startswith(command[1]):
								os.remove('sdfs_files/' + file)
					else:
						try:
							del_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
							del_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
							del_socket.connect((replica['id'][0], FILE_SYSTEM_RECVPORT))
						except socket.error as e: #If connection to remote machine fails
							print 'Could not connect to ' + str(replica['id'][0])
							conn.close()
							return

						del_socket.sendall(full_command)
				
				del self.file_list[sdfs_file_name]
				conn.sendall('File deleted')

				# send updated file_list to introducer
				sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				sock.sendto(json.dumps({'type': 'FILE_LIST', 'file_list': self.file_list}), (INTRODUCER_IP, INTRODUCER_PORT))
			else:
				# delete all files that start with the file name (should delete all versions)
				
				file_list = os.listdir('sdfs_files')

				for file in file_list:
					if file.startswith(command[1]):
						os.remove('sdfs_files/' + file)
			
			conn.close()
										
	# thread for PUT
	def threadHandleReplica(self, replica, local_file_name, sdfs_file_name, host):
		scp_command = 'scp mmahmad3@{source}:~/cs425mp3/local_files/{source_file_name} mmahmad3@{replica_node}:~/cs425mp3/sdfs_files/{sdfs_file_name}'.format(source = host, source_file_name = local_file_name, replica_node = replica['id'][0], sdfs_file_name = sdfs_file_name)

		try:
			retData = subprocess.check_call(scp_command, shell=True, stdout = FNULL, stderr = FNULL)
		except:
			self.queue.put(1)

		return
				
def printTable(myDict, colList=None):
	""" Pretty print a list of dictionaries (myDict) as a dynamically sized table.
	If column names (colList) aren't specified, they will show in random order.
	"""
	if not colList: colList = list(myDict[0].keys() if myDict else [])
	myList = [colList] # 1st row = header
	for item in myDict: myList.append([str(item[col] or '') for col in colList])
	colSize = [max(map(len,col)) for col in zip(*myList)]
	formatStr = ' | '.join(["{{:<{}}}".format(i) for i in colSize])	
	myList.insert(1, ['-' * i for i in colSize]) # Seperating line
	for item in myList: print(formatStr.format(*item))

def main():
	node = Node()

	t1 = threading.Thread(target = node.server, args = ())  
	t1.daemon = True  
	t1.start()

	t2 = threading.Thread(target = node.client, args = ())
	t2.daemon = True 
	t2.start()

	t3 = threading.Thread(target = node.fileSystemServer, args = ())  
	t3.daemon = True  
	t3.start()

	init = True
	node.command = 'JOIN'
	node.introduce()
	# Take input "JOIN/LEAVE/LIST/ID"
	'''
	while True:
		full_command = raw_input('\nEnter command...\n')

		if full_command == '':
			continue
			
		command = full_command.strip().split()

		if command[0].upper() == "JOIN":
			# Only join if not joined before
			if init:
				node.command = 'JOIN'
				node.introduce()

			init = False
		elif command[0].upper() == 'LEAVE':
			node.command = 'LEAVE'
			time.sleep(3)
			break
		elif command[0].upper() == 'LIST':
			printTable(node.membership_list, ['id'])
		elif command[0].upper() == 'ID':
			print node.id

		# GET <dest_file_name> <src_file_name>
		elif command[0].upper() == 'GET' or command[0].upper() == 'G':
			if len(command) != 3:
				print 'Invalid usage - GET needs 2 arguments'
				continue
			
			# contact master and send file
			master_socket = None
			master_host = node.master_id[0]
			master_port = FILE_SYSTEM_RECVPORT
			try:
				master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				master_socket.connect((master_host, master_port))
			except socket.error as e: #If connection to remote machine fails
				print 'Could not connect to ' + str(master_host)
				return

			try:
				master_socket.sendall(full_command)
				start_time = time.time()
				ret_value = master_socket.recv(1024)
				
				if ret_value.strip() == 'ACK':
					end_time = time.time()
					print 'File successfully downloaded from SDFS'
					print 'Time taken to upload file: '
					print end_time - start_time			

				elif ret_value.strip() == 'No such file exists':
					print 'File does not exist in SDFS'
					
			except socket.error as e:
				print 'Error during GET'

		# PUT <src_file_name> <dest_file_name>	
		elif command[0].upper() == 'PUT' or command[0].upper() == 'P':
			if len(command) != 3:
				print 'Invalid usage - PUT needs 2 arguments'
				continue																	
			
			# Check if file exists locally
			if not os.path.isfile('local_files/' + command[1]):
				print 'Local file does not exist'
				continue

			# contact master and send file
			master_socket = None
			master_host = node.master_id[0]
			master_port = FILE_SYSTEM_RECVPORT
			try:
				master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				master_socket.connect((master_host, master_port))
			except socket.error as e: #If connection to remote machine fails
				print 'Could not connect to ' + str(master_host)
				return

			try:
				master_socket.sendall(full_command)
				start_time = time.time()
				ret_value = master_socket.recv(1024)

				if ret_value.strip() == 'ACK':
					end_time = time.time()
					print 'File successfully uploaded to SDFS'	
					print 'Time taken to upload file: '
					print end_time - start_time												
			except socket.error as e:
				print 'Error during PUT'

		# STORE (list of all files at client)
		elif command[0].upper() == 'STORE' or command[0].upper() == 'S':
			file_list = os.listdir('sdfs_files')
			print('Files stored at this node:')
			print('-------------------------------------------')

			for file in file_list:
				print file

		# LS <file_name> (list all replicas where this file is stored)
		elif command[0].upper() == 'LS':
			
			if len(command) != 2:
				print "Invalid usage - LS needs an argument"
				continue

			# contact master and send file
			master_socket = None
			master_host = node.master_id[0]
			master_port = FILE_SYSTEM_RECVPORT
			try:
				master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				master_socket.connect((master_host, master_port))
			except socket.error as e: #If connection to remote machine fails
				print 'Could not connect to ' + str(master_host)
				return

			try:
				master_socket.sendall(full_command)
				ret_value = json.loads(master_socket.recv(1024))
																										
			except socket.error as e:
				print 'Error during LS'

			if ret_value[0] == 'No such file exists':
				print 'File you tried to LS does not exist'
				continue

			print('Replica nodes where {} is stored:'. format(command[1]))
			print('------------------------------------------------')

			for replica in ret_value:
				print replica

		# delete <file_name>	
		elif command[0].upper() == 'DELETE' or command[0].upper() == 'D':
			if len(command) != 2:
				print 'Invalid usage - DELETE needs 1 argument'
				continue
			
			# contact master and send file
			master_socket = None
			master_host = node.master_id[0]
			master_port = FILE_SYSTEM_RECVPORT
			try:
				master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				master_socket.connect((master_host, master_port))
			except socket.error as e: #If connection to remote machine fails
				print 'Could not connect to ' + str(master_host)
				return

			try:
				master_socket.sendall(full_command)
				ret_value = master_socket.recv(1024)

				if ret_value.strip() == 'File deleted':
					print 'File successfully deleted'
				elif ret_value.strip() == 'File does not exist':
					print 'Cannot delete file - does not exist'

			except socket.error as e:
				print 'Error during DELETE command'

		# GV <file_name> <num_version> <local file name> (get last x versions of the file)					
		elif command[0].upper() == 'GET-VERSIONS' or command[0].upper() == 'GV':
			if len(command) != 4:
				print 'Invalid usage - GET-VERSIONS needs 3 arguments'
				continue
			
			# contact master and send file
			master_socket = None
			master_host = node.master_id[0]
			master_port = FILE_SYSTEM_RECVPORT
			try:
				master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				master_socket.connect((master_host, master_port))
			except socket.error as e: #If connection to remote machine fails
				print 'Could not connect to ' + str(master_host)
				return

			try:
				master_socket.sendall(full_command)
				start_time = time.time()
				ret_value = master_socket.recv(1024)
			except socket.error as e:
				print 'Error during GET-VERSIONS'
				continue

			if ret_value.strip() == 'No such file exists':
				print 'File does not exist in SDFS'
			elif ret_value.strip() == 'All versions sent':
				file_list = os.listdir('local_files')
				
				with open('local_files/' + command[3], 'w') as write_file:
					for file in file_list:
						if file.startswith(command[1]):
							write_file.write('----------------------\n')
	
							with open('local_files/' + file) as read_file:
								lines = read_file.read()
								write_file.write(lines)

							os.remove('local_files/' + file)
				
				end_time = time.time()
				print 'Time taken to download versions: '
				print end_time - start_time
				print 'All versions transferred successfully'
	'''
	if node.command == 'LEAVE':
		return

	t1.join()
	t2.join()

if __name__ == '__main__':
	main()