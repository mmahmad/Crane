
def main():
	with open('input/airline-5k.csv') as infile:
		content = infile.readlines()

	content = [line.strip().split(',') for line in content]
	count = 0

	for row in content:
		if row[16] == 'LAX' and int(row[14]) > 0:
			count += 1

	print count

if __name__ == '__main__':
	main()