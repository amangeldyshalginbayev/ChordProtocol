import csv
import numpy as np
import os

keyRange = np.int_(np.logspace(10, 14, num = 5, base = 2))
print('keys: {}'.format(keyRange))

nodeRange = np.int_(np.logspace(4, 8, num = 5, base = 2))
print('nodes: {}'.format(nodeRange))

iterRange = np.array([0, 1])
print('iters: {}'.format(iterRange))

countTensor = np.empty((len(keyRange), len(nodeRange), len(iterRange)))
print('countTensor has shape: {}'.format(countTensor.shape))

for k, key in enumerate(keyRange):
	for n, node in enumerate(nodeRange):
		for i, iteration in enumerate(iterRange):
			path = './count_steps_keys_{}_nodes_{}_iter_{}'.format(key, node, iteration)
			with open(path, newline = '') as csvfile:
				reader = csv.DictReader(csvfile)
				for row_num, row in enumerate(reader):
					print(row_num)
					if row_num == 0:
						print(row['num_steps'])
						countTensor[k, n, i] = np.float(row['num_steps'])

print(countTensor)
countMatrix = np.mean(countTensor, axis = 2)
print('countMatrix has shape: {}'.format(countMatrix.shape))
print(countMatrix)
np.save('countMatrix.npy', countMatrix)
np.save('countTensor.npy', countTensor)
