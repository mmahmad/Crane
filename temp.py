
def main():
	with open('input/modified_house_data.csv') as infile:
		content = infile.readlines()

	content = [line.strip().split(',') for line in content]
	count = 0

	for line in content:
		if int(line[0]) > 80000:
			count += 1

	print count

if __name__ == '__main__':
	main()