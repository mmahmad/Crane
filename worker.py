import socket
import json
import threading
import Queue
import time
import pprint
import random
import node
import functools
import operator
import multiprocessing

def get_process_hostname():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	return s.getsockname()[0]	
	s.close()

NIMBUS_HOSTNAME = 'fa18-cs425-g03-01.cs.illinois.edu'
NIMBUS_HOSTNAME_2 = 'fa18-cs425-g03-02.cs.illinois.edu'
NIMBUS_PORT = 20000

MY_HOSTNAME = get_process_hostname()
MY_PORT_LISTEN_FOR_JOB = 6000
MY_PORT_LISTEN_FOR_ACKS = 4999

FILE_SYSTEM_RECVPORT = 10000

'''
Forward tuple to children
'''
def forwardTupleToChildren(task_details, forward_tuple, sock):
	
	children = task_details['children_ip_port'] # list
	for child_ip, child_port in children:

		try:
			sock.sendto(json.dumps(forward_tuple), (child_ip, child_port))
		except Exception as e:
			print e
			print 'Unable to contact child'
			print str(child_ip) + ":" + str(child_port)
			return
			
class Supervisor(object):
	def __init__(self):
		self.buffer = {}
		# self.process_list = []

		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(1)
		# join the fun
		try:	
			data = {"type": "JOIN_WORKER"}
			sock.sendto(json.dumps(data), (NIMBUS_HOSTNAME, NIMBUS_PORT))
			print "MY_IP:"
			print MY_HOSTNAME
		except socket.timeout as e:
			print 'Unable to contact nimbus 1'
			return

			try:
				data = {"type": "JOIN_WORKER"}
				sock.sendto(json.dumps(data), (NIMBUS_HOSTNAME, NIMBUS_PORT))
				print "MY_IP:"
				print MY_HOSTNAME
			except socket.timeout as e:
				print 'Unable to contact nimbus 1'
				return
		
				#If nimbus 1 has failed, try to contact nimbus 2
				try:	
					data = {"type": "JOIN_WORKER"}
					sock.sendto(json.dumps(data), (NIMBUS_HOSTNAME_2, NIMBUS_PORT))
					print "MY_IP:"
					print MY_HOSTNAME
				except socket.timeout as e:
					print 'Unable to contact nimbus 2'
					return

					try:
						data = {"type": "JOIN_WORKER"}
						sock.sendto(json.dumps(data), (NIMBUS_HOSTNAME_2, NIMBUS_PORT))
						print "MY_IP:"
						print MY_HOSTNAME
					except socket.timeout as e:
						print 'Unable to contact nimbus 2'
						return

		t1 = threading.Thread(target = self.listen, args = ())
		t1.daemon = True
		t1.start()

		# start the node failure detector component and sdfs
		failure_detector_node = node.Node()
		failure_detector_node.start()

	def listen(self):
		print "Supervisor.listen() called"
		# set up socket to listen for incoming jobs
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind((MY_HOSTNAME, MY_PORT_LISTEN_FOR_JOB))

		while(1):
			# task_details, addr = sock.recvfrom(1024000)
			data, addr = sock.recvfrom(1024000)
			t = threading.Thread(target = self.process_supervisor_message, args = (data, ))
			t.daemon = True
			t.start()

	def process_supervisor_message(self, data):
		print "received data"
		data = json.loads(data)
			
		if data['type'].upper() == 'NEW':
			task_details = data['task_details']
			worker_id = task_details['worker_id']
			
			# for process_id in self.process_list:
			# 	process_id.terminate()

			if task_details['type'] == 'spout':
				spout = Spout(task_details)
				self.buffer[worker_id] = spout
				t_id = threading.Thread(target = spout.start)
				t_id.daemon = True
				t_id.start()

			elif task_details['type'] == 'bolt':
				bolt = Bolt(task_details)
				self.buffer[worker_id] = bolt
				t_id = threading.Thread(target = bolt.start)
				t_id.daemon = True
				t_id.start()

		elif data['type'].upper() == 'UPDATE':
			pprint.pprint(data['task_details'])
			task_details = data['task_details']
			worker_id = task_details['worker_id']
			print "self.buffer[worker_id].task_details before:"
			print self.buffer[worker_id].task_details																						
			self.buffer[worker_id].task_details = task_details
			print "self.buffer[worker_id].task_details after:"
			print self.buffer[worker_id].task_details
		elif data['type'].upper() == 'SPOUT_UPDATE':
			task_details = data['task_details']
			worker_id = task_details['worker_id']
			self.buffer[worker_id].spout_ip = task_details['spout_ip_port'][0]
			self.buffer[worker_id].spout_port = task_details['spout_ip_port'][1]

'''
Spout is started by a supervisor;
It reads a given source (file/db/etc) line-by-line, converts it into tuples, and forwards to child(ren)
'''
class Spout(object):
	def __init__(self, task_details):
		self.task_details = task_details
		self.buffer = dict()

		# socket for sending tuples to child to avoid creating socket for each tuple
		self.send_to_child_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.send_to_child_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		# set to False when KILL_SPOUT message received. This is to stop the spout from forwarding tuples further.
		self.is_not_killed = True
		self.KILL_SPOUT_PORT = 6543
		self.kill_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.kill_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.kill_sock.bind((get_process_hostname(), self.KILL_SPOUT_PORT))
	
	def start(self):
		print "Spout started"
		print self.task_details

		t1 = threading.Thread(target = self.send_data)
		t1.daemon = True
		t1.start()

		t2 = threading.Thread(target=self.listen_for_kill_command)
		t2.daemon = True
		t2.start()

	def send_data(self):
		start_poll = False													
		tuple_id = 0 # incremented tuple_id added to each tuple

		print "spout reading and sending data..."
		# read data line-by-line from source, add msgId, and forward to child bolt(s)
		with open('input/' + self.task_details['input']) as infile:
			for line in infile:
				# check if spout has not been killed
				if self.is_not_killed:
					line = line.rstrip()

					# split line and store as tuple
					forward_tuple = tuple(line.split(','))
					print forward_tuple
					
					# store the tuple in buffer	
					self.buffer[tuple_id]= {
						'tuple_id': tuple_id,
						'tuple': forward_tuple,
						'timestamp': time.time() # epoch. number of seconds.
					}
					
					# forward the tuple to child bolt(s)
					forwardTupleToChildren(self.task_details, self.buffer[tuple_id], self.send_to_child_sock)
					time.sleep(0.01)

					tuple_id += 1
				else:
					break

		if self.is_not_killed:
			print "spout sent all data from file"

			time.sleep(2)
			# after all tuples sent, send 'EXIT' to all children
			EXIT_TUPLE = {
						'tuple_id': 'EXIT',
						'tuple': None,
						'timestamp': None
					}
			forwardTupleToChildren(self.task_details, EXIT_TUPLE, self.send_to_child_sock)
			print 'Spout shutting down...'
			return
		
		else:
			print "Spout streaming stopped due to KILL_SPOUT"

	def listen_for_kill_command(self):
		while True:
			data, addr = self.kill_sock.recvfrom(1000)

			if data.strip() == 'KILL_SPOUT':
				self.is_not_killed = False

class Bolt(object):
	def __init__(self, task_details):
		self.task_details = task_details
		self.listen_port = self.task_details['listen_port']
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind((get_process_hostname(), self.listen_port))
		self.queue = Queue.Queue()
		self.function = eval(self.task_details['function'])
		self.output_file = None # initialized in start()
		self.spout_ip, self.spout_port = self.task_details['spout_ip_port']
		self.send_to_child_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.send_to_child_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		self.send_ack_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.send_ack_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		self.written_tuples = set()
		self.client_ip_port = self.task_details['client_ip_port']

		self.state = None

	def start(self):
		# open output file and save handle if sink
		if self.task_details['sink']:
			# get file name
			output_filename = self.task_details['output']
			self.output_file = open("local_files/" + output_filename, 'w', 0) # 0 to write to file immediately

		# Start two threads, one to listen for new jobs, the other to start the bolts for it
		# 		
		t1 = threading.Thread(target = self.listen)
		t1.daemon = True
		t1.start()  

		t2 = threading.Thread(target = self.process_and_send)
		t2.daemon = True
		t2.start()

	def listen(self):
		print "Waiting for tuples..."

		while (1):
			# time.sleep(0.001)
			data, addr = self.sock.recvfrom(1024000)
			data = json.loads(data)
			self.queue.put(data)
				
	def process_and_send(self):
		while True:
			item = self.queue.get()
			tuple_id, tuple_data = item['tuple_id'], item['tuple']

			if tuple_id == 'EXIT':
				if self.task_details['sink']:

					if self.task_details['function_type'] == 'aggregate':
						self.output_file.write(str(self.state))
						self.output_file.write('\n')
					
					self.saveResults()

					data = {
						'type': 'JOB_COMPLETED',
						'master_ip': self.task_details['file_system_master'],
						'output_file' : self.task_details['output']
					}
					self.send_to_child_sock.sendto(json.dumps(data), (self.client_ip_port[0], self.client_ip_port[1]))
					return
				else:	
					forwardTupleToChildren(self.task_details, item, self.send_to_child_sock)
					print "EXIT received. Bolt shutting down..."
					return

			# if bolt function is a filter, returns a boolean for each tuple
			if self.task_details['function_type'] == 'filter':
				output = self.function(tuple_data)
				if output: # if true, forward to next bolt
					print item
					if self.task_details['sink']:
						# if tuple was already written, do not write to file again
						if tuple_id in self.written_tuples:
							pass
						else:
							self.output_file.write((output.encode('utf-8')))
							self.output_file.write('\n')
					else:
						forwardTupleToChildren(self.task_details, item, self.send_to_child_sock)
				else:
					pass
			elif self.task_details['function_type'] == 'transform':
				output = self.function(tuple_data)
				print output
				if output:
					if self.task_details['sink']:
						# send ACK+REMOVE message to spout
						if tuple_id in self.written_tuples:
							print 'Line already written, ignore'
						else:
							self.written_tuples.add(tuple_id)			
							self.output_file.write((output.encode('utf-8')))
							self.output_file.write('\n')
					else:
						item['tuple'] = output						
						forwardTupleToChildren(self.task_details, item, self.send_to_child_sock)
					
			elif self.task_details['function_type'] == 'aggregate':
				if self.state is None:
					self.state = tuple_data
				else:
					self.state = functools.reduce(self.function, [self.state, tuple_data])
					print 'Intermediate output at sink: ' + str(self.state)
			
			elif self.task_details['function_type'] == 'join':
				join_columns = self.task_details['join_columns']
				join_file = self.task_details['join_file']

				with open(join_file, 'r') as jf:
					lines = jf.readlines()
					for row in lines:
						if tuple_data[join_columns] == lines[join_columns]:
							#Matches condition, join tuple and forward to children
							new_tuple = tuple_data + lines

							item['tuple'] = new_tuple

							if self.task_details['sink']:
								self.output_file.write(output.encode('utf-8'))
								self.output_file.write('\n')
							else:
								forwardTupleToChildren(self.task_details, item, self.send_to_child_sock)


	def saveResults(self):
		master_socket = None
		master_host = self.task_details['file_system_master']
		master_port = FILE_SYSTEM_RECVPORT
			
		try:
			master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			master_socket.connect((master_host, master_port))
		except socket.error as e: #If connection to remote machine fails
			print 'Could not connect to ' + str(master_host)
			return

		full_command = 'PUT ' + self.task_details['output'] + ' ' + self.task_details['output']
		print full_command
		
		try:
			master_socket.sendall(full_command)
			start_time = time.time()
			ret_value = master_socket.recv(1024)

			if ret_value.strip() == 'ACK':
				end_time = time.time()
				print 'Output successfully uploaded to SDFS'	
				print 'Time taken to upload file: '
				print end_time - start_time												
		except socket.error as e:
			print 'Error during PUT'
		
def main():
	# supervisor connects to nimbus to let it know that it is available
	supervisor = Supervisor()
	# supervisor.listen()

if __name__ == '__main__':
	main()

