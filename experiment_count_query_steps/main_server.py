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


# RMB: added this function to test average number of steps following query.
#variables input through argparser in main_server.py, but not here

def return_query_steps(num_keys, num_nodes):
    num_steps_between_new_nodes = 20 * c.max_offset

    num_queries = 1000
    steps_between_query = 1
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

    # query
    for i in range(num_queries * steps_between_query):
        if i % (steps_between_query) == 0:
            item_key = random.sample(chord.item_keys, 1)[0]
            chord.query_item(item_key)
        chord.advance_all_one_step(verbose=verbose)

    # to make sure all have cleared (max transit should be one time round)
    for i in range(num_queries * c.ring_size):
        chord.advance_all_one_step(verbose=verbose)
    average_steps = mean(chord.step_tracker)
    print('\n Average number of steps was {} for {} nodes with {} keys total!'.format(average_steps,
        num_nodes, num_keys))
    return average_steps

# helper function to write to csv file. variables input through parser in header of main_server.py, but not here
def output_steps(num_keys, num_nodes, iteration, output_path):
    trial_steps = [num_keys, num_nodes, return_query_steps(num_keys, num_nodes), iteration]

    # we are saving here
    path_string ='{}/count_steps_keys_{}_nodes_{}_iter_{}'.format(output_path, num_keys, num_nodes, iteration)

    # remove any old version
    if os.path.exists(path_string):
       os.remove(path_string)

    # dump to csv
    with open(path_string, 'w') as myfile:
       wr = csv.writer(myfile)
       wr.writerow(['num_keys', 'num_nodes', 'num_steps', 'iteration'])
       wr.writerow(trial_steps)

if __name__ == "__main__":
    # input variables fed in through parser	
    output_steps(num_keys, num_nodes, iteration, "result_dump")
