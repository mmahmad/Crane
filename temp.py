
def main():
	with open('input/airline-5k.csv') as infile:
		content = infile.readlines()

	content = [line.strip().split(',') for line in content]
	count = 0

	for line in content:
		print line[14]
		
	print count

if __name__ == '__main__':
	main()