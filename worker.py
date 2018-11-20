import socket
import json
import threading
import Queue
import time

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
def forwardTupleToChildren(task_details, forward_tuple):
	children = task_details['children_ip_port'] # list
	for child_ip, child_port in children:
		# forward
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			sock.sendto(json.dumps(forward_tuple), (child_ip, child_port))
		except Exception as e:
			print e
			print 'Unable to contact child'
			print str(child_ip) + ":" + str(child_port)
			return
			
class Supervisor(object):
	def __init__(self):
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

	def listen(self):
		# set up socket to listen for incoming jobs
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind((MY_HOSTNAME, MY_PORT_LISTEN_FOR_JOB))

		while(1):
			task_details, addr = sock.recvfrom(1024000)
			print "received task"
			task_details = json.loads(task_details)

			if task_details['type'] == 'spout':
				spout = Spout(task_details)
				t_id = threading.Thread(target = spout.start)
				t_id.daemon = True
				t_id.start()
			elif task_details['type'] == 'bolt':
				bolt = Bolt(task_details)
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
		# UDP
		#---------
		self.ack_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.ack_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.ack_sock.bind((get_process_hostname(), MY_PORT_LISTEN_FOR_ACKS))

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
				forwardTupleToChildren(self.task_details, self.buffer[tuple_id])
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
			data, addr = self.ack_sock.recvfrom(1024)

			# data = client_socket.recv(1024)
			received_data = json.loads(data)

			print 'ack received for tuple with id: ' + str(data['tuple_id'])
			
			# TCP
			# client_socket, address = self.ack_sock.accept()
			
			t = threading.Thread(target = self.process_acks, args=(received_data,))
			t.daemon = True
			t.start()

	'''
	received_data: 
		{
			'type': 'KEEP' / 'REMOVE'
			'tuple_id': 31
		}
	'''
	def process_acks(self, received_data):

		# If received data has type=='KEEP', update timestamp
		if received_data['type'].upper() == 'KEEP':
			self.buffer[received_data['tuple_id']]['timestamp'] = time.time()

		# else if received data has type=='REMOVE', remove from buffer
		elif received_data['type'].upper() == 'REMOVE':
			print 'length of buffer before removal: '
			print len(self.buffer)
			del self.buffer[received_data['tuple_id']]
			print 'length of buffer after removal: '
			print len(self.buffer)

	def check_timeouts(self):
		# TODO:
		pass

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
		self.tuple_counter = 0

	def send_ack(self, tuple_id, msg_type):
		# send ACK+REMOVE message to spout
		ack_message = {
			'type': msg_type,
			'tuple_id': tuple_id
		}
		
		# TCP
		# self.ack_socket.sendall(json.dumps(ack_message))
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			sock.sendto(json.dumps(ack_message), (self.spout_ip, self.spout_port))
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
			data, addr = self.sock.recvfrom(1024)
			data = json.loads(data)
			self.queue.put(data)
				
	def process_and_send(self):
		while True:
			item = self.queue.get()
			tuple_id, tuple_data = item['tuple_id'], item['tuple']
			print "item"
			print tuple_id, tuple_data

			# if bolt function is a filter, returns a boolean for each tuple
			if self.task_details['function_type'] == 'filter':
				output = self.function(tuple_data)
				if output: # if true, forward to next bolt
					if self.task_details['sink']:
						self.output_file.write((output.encode('utf-8')))
						self.output_file.write('\n')
						self.send_ack(tuple_id, 'REMOVE')
					else:
						# send ACK+KEEP message to spout
						self.send_ack(tuple_id, 'KEEP')
						forwardTupleToChildren(self.task_details, item)
				else:
					print 'send ACK+REMOVE for filtered tuple'
					self.send_ack(tuple_id, 'REMOVE') # in case tuple has been filtered out, spout no longer needs to keep track of this tuple

			elif self.task_details['function_type'] == 'transform':
				output = self.function(tuple_data)
				if output:
					if self.task_details['sink']:
						# send ACK+REMOVE message to spout
						self.output_file.write((output.encode('utf-8')))
						self.output_file.write('\n')
						self.send_ack(tuple_id, 'REMOVE')
					else:
						item['tuple'] = output
						# send ACK+KEEP message to spout
						forwardTupleToChildren(self.task_details, item)
						self.send_ack(tuple_id, 'KEEP')						
			elif self.task_details['function_type'] == 'join':
				#TODO: join()  
				pass
		
def main():
	# supervisor connects to nimbus to let it know that it is available
	supervisor = Supervisor()
	supervisor.listen()

if __name__ == '__main__':
	main()

