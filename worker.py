import socket
import json
import threading
import Queue
import time
import pprint
import random
import node

def get_process_hostname():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	return s.getsockname()[0]	
	s.close()

NIMBUS_HOSTNAME = 'fa18-cs425-g03-01.cs.illinois.edu'
NIMBUS_PORT = 20000

MY_HOSTNAME = get_process_hostname()
MY_PORT_LISTEN_FOR_JOB = 6000
MY_PORT_LISTEN_FOR_ACKS = 4999

'''
Forward tuple to children
'''
def forwardTupleToChildren(task_details, forward_tuple, sock):
	# TODO: If current worker is spout and it has multiple children, duplicate tuples should have separate unique tuple_id
	children = task_details['children_ip_port'] # list
	for child_ip, child_port in children:
		# forward
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

		# join the fun
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			data = {"type": "JOIN_WORKER"}
			sock.sendto(json.dumps(data), (NIMBUS_HOSTNAME, NIMBUS_PORT))
			print "MY_IP:"
			print MY_HOSTNAME
		except:
			print 'Unable to contact nimbus'
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
			print "received data"
			data = json.loads(data)
			
			if data['type'].upper() == 'NEW':
				task_details = data['task_details']
				worker_id = task_details['worker_id']
				
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

'''
Spout is started by a supervisor;
It reads a given source (file/db/etc) line-by-line, generates a unique msgId for each line and appends the msgId to it;
Store the line in buffer (dict with msgId as key for fast search) with timestamp;
Convert line to tuple and forward to child(ren) bolt(s);

ACTIONS ON BUFFER
-----------------
Update timestamp on ACK+KEEP;
Update timestamp and re-send if no ack from child(ren);
Remove if ACK+REMOVE received;
'''
class Spout(object):
	def __init__(self, task_details):
		self.task_details = task_details
		self.buffer = dict()
		self.MAX_ACK_TIMEOUT = 1 # 1 second timeout		
		# UDP
		#---------
		self.ack_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.ack_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.ack_sock.bind((get_process_hostname(), MY_PORT_LISTEN_FOR_ACKS))

		# socket for sending tuples to child to avoid creating socket for each tuple
		self.send_to_child_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.send_to_child_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		# TCP
		#---------
		# self.ack_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		# self.ack_sock.bind((socket.gethostname(), MY_PORT_LISTEN_FOR_ACKS))
		# self.ack_sock.listen(5) # listen to max 5 bolts'
		
	
	def start(self):
		print "Spout started"
		print self.task_details

		t1 = threading.Thread(target = self.send_data)
		t1.daemon = True
		t1.start()

		t2 = threading.Thread(target = self.listen_for_acks)
		t2.daemon = True
		t2.start()

	def send_data(self):
		start_poll = False													
		tuple_id = 0 # incremented tuple_id added to each tuple

		# read data line-by-line from source, add msgId, and forward to child bolt(s)
		with open('input/' + self.task_details['input']) as infile:
			for line in infile:
				line = line.rstrip()

				# split line and store as tuple
				forward_tuple = tuple(line.split(','))

				# store the tuple in buffer	
				self.buffer[tuple_id]= {
					'tuple_id': tuple_id,
					'tuple': forward_tuple,
					'timestamp': time.time() # epoch. number of seconds.
				}
				
				# forward the tuple to child bolt(s)
				forwardTupleToChildren(self.task_details, self.buffer[tuple_id], self.send_to_child_sock)
				time.sleep(0.1)

				if not start_poll:
					timeout_thread = threading.Thread(target = self.check_timeouts, args = ())
					timeout_thread.daemon = True
					timeout_thread.start()
					start_poll = True

				tuple_id += 1
		
		while True:
			print 'length of buffer'
			print len(self.buffer)
			time.sleep(3)
			if len(self.buffer) == 0:
				print 'Job completed'
				return

	def listen_for_acks(self):
		while(1):
			# UDP			
			data, addr = self.ack_sock.recvfrom(1024000)
			
			# TCP
			# client_socket, address = self.ack_sock.accept()
			
			t = threading.Thread(target = self.process_acks, args=(data,))
			t.daemon = True
			t.start()

	'''
	received_data: 
		{
			'type': 'KEEP' / 'REMOVE'
			'tuple_id': 31
		}
	'''
	def process_acks(self, data):

		# data = client_socket.recv(1024)
		received_data = json.loads(data)

		print 'ack received for tuple with id: ' + str(received_data['tuple_id'])
		
		# If received data has type=='KEEP', update timestamp
		if received_data['type'].upper() == 'KEEP':
			self.buffer[received_data['tuple_id']]['timestamp'] = time.time()

		# else if received data has type=='REMOVE', remove from buffer
		elif received_data['type'].upper() == 'REMOVE':
			# print 'length of buffer before removal: '
			# print len(self.buffer)
			del self.buffer[received_data['tuple_id']]
			# print 'length of buffer after removal: '
			# print len(self.buffer)

	'''
	Continuosly loop through buffer to see if any bolt has timed-out (ack not received).
	Re-send if timeout > MAX_ACK_TIMEOUT
	'''
	def check_timeouts(self):
		#while len(self.buffer) > 0:
		while True:
			buffer_copy = self.buffer.copy()			
			for tuple_id, tuple_data in buffer_copy.items():
				current_time = time.time()
				if current_time - tuple_data['timestamp'] > self.MAX_ACK_TIMEOUT:
					# check if tuple is still there in self.buffer (possible that it was removed by now due to listen_for_ack thread's action)
					if tuple_id in self.buffer:
					# re-send tuple
						self.buffer[tuple_id]['timestamp'] = current_time
						forwardTupleToChildren(self.task_details, tuple_data, self.send_to_child_sock)
						print 'Resent tuple' + str(tuple_id)
					else:
						print "Cannot update timestamp. Tuple already deleted!"

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
		
	def send_ack(self, tuple_id, msg_type):
		# send ACK+REMOVE message to spout
		ack_message = {
			'type': msg_type,
			'tuple_id': tuple_id
		}
		
		# TCP
		# self.ack_socket.sendall(json.dumps(ack_message))
		try:
			self.send_to_child_sock.sendto(json.dumps(ack_message), (self.spout_ip, self.spout_port))
		except Exception as e:
			print e
			print 'Unable to contact spout'
			return

	def start(self):
		# open output file and save handle if sink
		if self.task_details['sink']:
			# get file name
			output_filename = self.task_details['output']
			self.output_file = open(output_filename, 'w', 0) # 0 to write to file immediately

							
		#Create outgoing TCP connection to send ACKs
		# try:
		# 	self.ack_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		# 	self.ack_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		# 	self.ack_socket.connect((self.spout_ip, self.spout_port))
		# except socket.error as e: #If connection to remote machine fails
		# 	print 'Could not connect to ' + str(self.spout_ip)
		# 	return
				
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
			# print "item"
			# print tuple_id, tuple_data

			# if bolt function is a filter, returns a boolean for each tuple
			if self.task_details['function_type'] == 'filter':
				output = self.function(tuple_data)
				if output: # if true, forward to next bolt
					if self.task_details['sink']:
						# if tuple was already written, do not write to file again
						if tuple_id in self.written_tuples:
							print 'Line already written, ignore'
							self.send_ack(tuple_id, 'REMOVE')
						else:
							# self.written_tuples.add(tuple_id)
							# print 'List of tuple_ids already written is'
							print self.written_tuples
							self.output_file.write((output.encode('utf-8')))
							self.output_file.write('\n')
							print 'send ACK+REMOVE for filtered tuple'
							self.send_ack(tuple_id, 'REMOVE')
					else:
						# send ACK+KEEP message to spout
						print 'send ACK+KEEP for filtered tuple'
						self.send_ack(tuple_id, 'KEEP')

						forwardTupleToChildren(self.task_details, item, self.send_to_child_sock)
				else:
					print 'send ACK+REMOVE for filtered tuple'
					self.send_ack(tuple_id, 'REMOVE') # in case tuple has been filtered out, spout no longer needs to keep track of this tuple

			elif self.task_details['function_type'] == 'transform':
				output = self.function(tuple_data)
				if output:
					if self.task_details['sink']:
						# send ACK+REMOVE message to spout
						if tuple_id in self.written_tuples:
							print 'Line already written, ignore'
							self.send_ack(tuple_id, 'REMOVE')
						else:
							self.written_tuples.add(tuple_id)
							print 'List of tuple_ids already written is'
							print self.written_tuples					
							self.output_file.write((output.encode('utf-8')))
							self.output_file.write('\n')
							print 'send ACK+REMOVE for transformed tuple'
							self.send_ack(tuple_id, 'REMOVE')
					else:
						item['tuple'] = output
						# send ACK+KEEP message to spout
						forwardTupleToChildren(self.task_details, item, self.send_to_child_sock)
						print 'send ACK+KEEP for transformed tuple'
						self.send_ack(tuple_id, 'KEEP')						
			elif self.task_details['function_type'] == 'join':
				#TODO: join()  
				pass
		
def main():
	# supervisor connects to nimbus to let it know that it is available
	supervisor = Supervisor()
	# supervisor.listen()

if __name__ == '__main__':
	main()

