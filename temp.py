
def main():
	with open('input/airline-5k.csv') as infile:
		content = infile.readlines()

	content = [line.strip().split(',') for line in content]
	count = 0

	for line in content:
		if int(line[14]) > 0 and line[16] == 'LAX':
			count += 1

	print count

if __name__ == '__main__':
	main()