import socket
import yaml
import json
import time

NIMBUS_PORT = 20000
NIMBUS_HOST = 'fa18-cs425-g03-01.cs.illinois.edu'
NIMBUS_HOST_2 = 'fa18-cs425-g03-02.cs.illinois.edu'

LISTEN_PORT = 6789 # gets message when job completes
FILE_SYSTEM_RECVPORT = 10000

def get_process_hostname():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(("8.8.8.8", 80))
	return s.getsockname()[0]	
	s.close()

def main():
	
	#start <config_file_name>
	cmd = raw_input('Enter command: \n')
	cmd += ".yaml"

	# get file name
	config_file = cmd.strip().split(' ')[1]
	start_time = time.time()

	if cmd.strip().split(' ')[0] == 'start':
		# pass file to Nimbus
		with open(config_file) as cf:
			config = yaml.load(cf)

		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(1)

		try:
			data = {"type": "START_JOB", "config": config}
			sock.sendto(json.dumps(data), (NIMBUS_HOST, NIMBUS_PORT))   
		except socket.timeout as e:
			try:
				data = {"type": "START_JOB", "config": config}
				sock.sendto(json.dumps(data), (NIMBUS_HOST, NIMBUS_PORT))
			except socket.timeout as e:
				print 'Cannot start job, connection error'
				return

	if cmd.strip().split(' ')[0] == 'leave':
		return
	
	listenForResult()
	end_time = time.time()

	print 'Time taken to run job: ' + str(end_time - start_time)
	
def listenForResult():
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	sock.bind((get_process_hostname(), LISTEN_PORT))

	while True:
		data, addr = sock.recvfrom(1024)
		data = json.loads(data)
		if data['type'].upper() == 'JOB_COMPLETED':
			print 'Job Completed. Collecting results...'

			sdfs_master_ip = data['master_ip']
			sdfs_master_port = FILE_SYSTEM_RECVPORT
			sdfs_file_name = data['output_file']

			# Get result file
			getResultFile(sdfs_master_ip, sdfs_master_port, sdfs_file_name)
			return

def getResultFile(sdfs_master_ip, sdfs_master_port, sdfs_file_name):
	# contact master and send file
	master_socket = None
	master_host = sdfs_master_ip
	master_port = sdfs_master_port
	
	try:
		master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		master_socket.connect((master_host, master_port))
	except socket.error as e: #If connection to remote machine fails
		print 'Could not connect to ' + str(master_host)
		return

	full_command = 'GET ' + sdfs_file_name + ' ' + sdfs_file_name
	print full_command

	try:
		master_socket.sendall(full_command)
		start_time = time.time()
		ret_value = master_socket.recv(1024)
		
		if ret_value.strip() == 'ACK':
			end_time = time.time()
			print 'Output successfully downloaded from SDFS'
		elif ret_value.strip() == 'No such file exists':
			print 'File does not exist in SDFS'
			
	except socket.error as e:
		print 'Error during GET'

if __name__ == '__main__':
    main()
