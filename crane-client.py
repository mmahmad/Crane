import socket
import yaml
import json

NIMBUS_PORT = 20000
NIMBUS_HOST = 'fa18-cs425-g03-01.cs.illinois.edu'

def main():
	while True:
		#start <config_file_name>
		cmd = raw_input('Enter command: \n')
		cmd += ".yaml"

		# get file name
		config_file = cmd.strip().split(' ')[1]

		if cmd.strip().split(' ')[0] == 'start':
			# pass file to Nimbus
			with open(config_file) as cf:
				config = yaml.load(cf)

			sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			data = {"type": "START_JOB", "config": config}
			sock.sendto(json.dumps(data), (NIMBUS_HOST, NIMBUS_PORT))   

		if cmd.strip().split(' ')[0] == 'leave':
			return   

if __name__ == '__main__':
    main()
