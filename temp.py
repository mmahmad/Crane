
def main():
	with open('input/tweets-5k.csv') as infile:
		content = infile.readlines()

	content = [line.strip().split(',') for line in content]
	count = 0

	for line in content:
		if '@mileycyrus' in line[5]:
			count += 1

	print count

if __name__ == '__main__':
	main()