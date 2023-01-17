from ChordRing import ChordRing
import constants as c
import random
from statistics import mean 
import argparse
import os
import csv
verbose = False

parser = argparse.ArgumentParser()
parser.add_argument("-k", type = int)
parser.add_argument("-n", type = int)
parser.add_argument("-i", type = int)
args = parser.parse_args()
print(args)
num_keys = args.k
num_nodes = args.n
iteration = args.i
output_path = "result_dump"

def rand_key():
    return random.randint(0, 2 ** c.ring_size)

# RMB: necessary, really
def rand_key_without_replacement(currentKeyList):
    allChoices = set([x for x in range(2 ** c.ring_size)])
    return random.choice(list(allChoices - set(currentKeyList)))

# RMB: added below to avoid many collisions when number of nodes approaches ring size
def rand_address(nodeList):
    allChoices = set([x for x in range(2 ** c.ring_size)])
    return random.choice(list(allChoices - set(nodeList)))

# RMB: added this function to return number of keys per node 
#variables input through argparser in main_server.py, but not here

def key_distribution(num_keys, num_nodes):
    num_steps_between_new_nodes = 20 * c.max_offset
    
    # initialize ring
    chord = ChordRing()
    chord.add_node(rand_key())

    # RMB: now take key without replacement
    keyList = []
    for i in range(num_keys):
        key = rand_key_without_replacement(keyList)
        keyList.append(key)
        chord.add_item((key, key))
  
    keyList = None

    # add nodes
    for i in range(num_nodes * num_steps_between_new_nodes):
        if i % (num_steps_between_new_nodes) == 0:
            try:
                chord.add_node(rand_address(chord.nodeList))
            except Exception as e: 
                print('no more names to allocate (more nodes than chord size?): {}'.format(e))
        chord.advance_all_one_step()
    for i in range(c.max_offset*100):
        chord.advance_all_one_step()
    chord.stop_periodic=True
    print('No longer adding')

    # stabliize and check correctness
    for i in range(c.max_offset*200):
        chord.advance_all_one_step()
    print(chord)
    chord.check_correctness()

    # here is the juice
    keyDistributionList = chord.return_key_distribution()
    print('\n Average number of keys per node was {} for {} nodes with {} keys total!'.format(mean(keyDistributionList), 
        num_nodes, num_keys))
    return keyDistributionList

# helper function to write to csv file. variables input through parser in header of main_server.py, but not here
def output_key_distributions(num_keys, num_nodes, iteration, output_path):
    
    keyDistributionList = key_distribution(num_keys, num_nodes)

    # we are saving here
    path_string ='{}/key_distribution_keys_{}_nodes_{}_iter_{}'.format(output_path, num_keys, num_nodes, iteration) 

    # remove any old version
    if os.path.exists(path_string):
       os.remove(path_string)

    # dump to csv
    with open(path_string, 'w') as myfile:
       wr = csv.writer(myfile)
       
       wr.writerow(keyDistributionList)

if __name__ == "__main__":
    # input variables fed in through parser	
    output_key_distributions(num_keys, num_nodes, iteration, output_path)

