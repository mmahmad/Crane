import socket
import json
import threading

def get_process_hostname():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	return s.getsockname()[0]	
	s.close()

NIMBUS_HOSTNAME = 'fa18-cs425-g03-01.cs.illinois.edu'
NIMBUS_PORT = 20000

MY_HOSTNAME = get_process_hostname()
MY_PORT_LISTEN_FOR_JOB = 6000

class Supervisor(object):
	def __init__(self):
		# join the fun
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			data = {"type": "JOIN_WORKER"}
			sock.sendto(json.dumps(data), (NIMBUS_HOSTNAME, NIMBUS_PORT))
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
It reads a given source (file/db/etc) line-by-line, generates a unique msgId for each line and prepends the msgId to it;
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
	
	def start(self):
		print "Spout started"
		print self.task_details

		tuple_id = 0

		# read data line-by-line from source, add msgId, and forward to child bolt(s)
		with open('input/' + self.task_details['input']) as infile:
			for line in infile:
				line = line.rstrip()
				# add a unique (auto_incremented) id to the line
				line = str(tuple_id) + ',' + line

				# split line and store as tuple
				forward_tuple = tuple(line.split(','))

				children = self.task_details['children_ip_port'] # list
				for child_ip, child_port in children:
					# forward
					try:
						sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
						sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
						sock.sendto(json.dumps(forward_tuple), (child_ip, child_port))
					except:
						print 'Unable to contact child'
						print str(child_ip) + ":" + str(child_port)
						return

				tuple_id += 1


class Bolt(object):
	def __init__(self, task_details):
		self.task_details = task_details
		self.listen_port = self.task_details['listen_port']
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind((get_process_hostname(), self.listen_port))

	def start(self):
		t1 = threading.Thread(target = self.listen)
		t1.daemon = True
		t1.start()  
					
	def listen(self):
		print "Waiting for tuples..."

		while (1):
			data, addr = self.sock.recvfrom(1024)
			data = json.loads(data)
			print data

def main():
	# supervisor connects to nimbus to let it know that it is available
	supervisor = Supervisor()
	supervisor.listen()

if __name__ == '__main__':
	main()

