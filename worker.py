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
			data, addr = sock.recvfrom(1024000)
			print "received job"
			data = json.loads(data)

			if data['type'] == 'spout':
				spout = Spout(data)
				t_id = threading.Thread(target = spout.start)
				t_id.daemon = True
				t_id.start()
			elif data['type'] == 'bolt':
				bolt = Bolt(data)
				t_id = threading.Thread(target = bolt.start)
				t_id.daemon = True
				t_id.start()

class Spout(object):
	def __init__(self, config):
		self.config = config
	
	def start(self):
		print self.config

class Bolt(object):
	def __init__(self, config):
		self.config = config
	
	def start(self):
		print self.config

def main():
	# supervisor connects to nimbus to let it know that it is available
	supervisor = Supervisor()
	supervisor.listen()

if __name__ == '__main__':
	main()

