import constants as c
import random
import bisect

from Node import ChordNode, between
from collections import OrderedDict

class ChordRing:
    '''
    The overarching system that keeps track of the nodes. It contains the following:
    nodeDict: Dictionary that maps IDs to Nodes
    nodeList: List of Nodes in numerical order of their IDs.
    curr_node_ind: Index that keeps track of which node is next to process RPC.
    num_node: Keeps track of how many nodes are in the Chord Ring.
    step_tracker: list with number of steps per query. passed with initial query RPC (see ring query_item method)
    '''
    def __init__(self):
        self.nodeDict  = OrderedDict()
        self.nodeList  = []
        self.num_node  = 0
        self.item_keys = set()
        # RMB: initialize step tracker structure
        self.step_tracker = []
    '''
    Adds a node into the Ring, if there isn't already a node with the same values. 
    If the Node to be added is the first one, it will send the 'create' RPC to it.
    Otherwise, it will send the 'join' RPC.
    '''
    def add_node(self, ID):
        print('Adding node: {}'.format(ID))
        if ID in self.nodeDict.keys():
            print('Node already exists on the Chord ring. Exiting operation.')
            return

        # Initialize node, and add method of joining the chord ring.
        node = ChordNode(ID)
        if len(self.nodeDict) == 0:
            kwargs = {}
            function_RPC = ('create', kwargs)
            node.incoming_RPCs.append(('function', function_RPC))
        else:
            join_ID = random.choice(list(self.nodeDict.keys()))
            kwargs = {'join_ID': join_ID, 'nodeDict': self.nodeDict}
            function_RPC = ('join', kwargs)
            node.incoming_RPCs.append(('function', function_RPC))
        # Update the Chord Ring.
        self.nodeDict[ID] = node
        bisect.insort(self.nodeList, ID)
        self.num_node += 1

    '''
    Remove a node from the Ring. Because we are assuming spontaneous failure,
    all that happens is that the Node is no longer kept track of by the Chord Ring.
    '''
    def remove_node_failure(self, ID):
        print('Removing node by failure: {}'.format(ID))
        self.nodeDict.pop(ID)
        self.nodeList.remove(ID)
        self.num_node -= 1

    '''
    Remove a node from the Ring. Same as above, but this time we fail gracefully.
    This means that the contents of that are stored in the Node are first 
    transfered to the successor.
    '''
    def remove_node_graceful(self, ID):
        print('Removing node gracefully: {}'.format(ID))
        self.nodeDict[ID].send_successor_items(self.nodeDict)
        self.nodeDict.pop(ID)
        self.nodeList.remove(ID)
        self.num_node -= 1

    '''
    Has every node in the Chord Ring take one step.
    '''
    def advance_all_one_step(self, verbose=False):
        for i in range(self.num_node):
            node_ID = list(self.nodeDict.keys())[i]
            node    = self.nodeDict[node_ID]
 
            node.counter[0] += 1
            self.check_periodic_ops(node)
            node.process_incoming_RPC(self.nodeDict, verbose)


    '''
    There are three operations that each Node should periodically run. 
    This function checks if it's time to run it, and if so, send the RPC to do so.
    '''
    def check_periodic_ops(self, node):
        if (node.counter[0] + node.counter[1]) % c.stabilize_period == 0 and node.joined:
            kwargs = {'nodeDict': self.nodeDict}
            function_RPC = ('stabilize', kwargs)
            node.incoming_RPCs.append(('function', function_RPC))
        if (node.counter[0] + node.counter[1]) % c.fix_finger_period == 0 and node.joined:
            kwargs = {'nodeDict': self.nodeDict}
            function_RPC = ('fix_finger', kwargs)
            node.incoming_RPCs.append(('function', function_RPC))
        if (node.counter[0] + node.counter[1]) % c.check_pred_period == 0 and node.joined:
            kwargs = {'nodeDict': self.nodeDict}
            function_RPC = ('check_pred', kwargs)
            node.incoming_RPCs.append(('function', function_RPC))
    '''
    Add item to the Chord ring. Note that I am cheating by just directly adding
    it in, rather than finding the successor and having the successor add the item.
    '''
    def add_item(self, item):
        k, v = item
        self.item_keys.add(k)
        for i, curr_ID in enumerate(self.nodeList):
            prev_ID = self.nodeList[(i-1)%len(self.nodeList)]
            if between(prev_ID, curr_ID, k):
                self.nodeDict[curr_ID].var['storage'][k] = v
 
    # RMB: added step tracker to kwargs below
    def query_item(self, item_key):
        print('Querying for {}'.format(item_key))
        query_ID   = random.choice(list(self.nodeDict.keys()))
        query_node = self.nodeDict[query_ID]
        kwargs = {'dest_ID': query_ID, 'var_name': 'client', 'key': item_key, 
                  'nodeDict': self.nodeDict, 'steps':0, 'step_tracker': self.step_tracker}
        function_RPC = ('find_successor', kwargs)
        query_node.incoming_RPCs.append(('function', function_RPC))

    '''
    Checks to see if the current state of the Chord Ring is "correct".
    '''
    def check_correctness(self):
        key_list = list()
        overall_fail_test = False
        for node_ind, curr_ID in enumerate(self.nodeList):
            to_print = '\n'
            fail_test = False
            to_print += 'Problems with Node {}\n'.format(curr_ID) +'-'*30
            curr_node = self.nodeDict[curr_ID]

            # Check predecessor is correct
            e_pred_ID = self.nodeList[(node_ind-1)%len(self.nodeList)]
            a_pred_ID = curr_node.var['pred_ID'] 
            if not e_pred_ID == a_pred_ID:
                fail_test = True
                to_print += '\nNode {} has wrong predecessor. Expected: {}\tActual: {}'.format(curr_ID, e_pred_ID, a_pred_ID)

            # Check if values in storage are correct
            item_keys = list(curr_node.var['storage'].keys())
            for item_key in item_keys:
                j = (bisect.bisect_left(self.nodeList, item_key)) % len(self.nodeList)
                e_ID = self.nodeList[j]
                if not e_ID == curr_ID:
                    fail_test = True
                    to_print += '\nNode {} incorrectly possesses item with key {}. Expected Node: {}'.format(curr_ID, item_key, e_ID)
            
            # Check if finger table values are correct
            for ind in range(c.ring_size):
                key  = (curr_ID + 2**ind) % 2**c.ring_size
                ft_i = curr_node.var['finger_table_{}'.format(ind)]
                j = (bisect.bisect_left(self.nodeList, key)) % len(self.nodeList)
                e_ft_i = self.nodeList[j]
                if not ft_i == e_ft_i:
                    fail_test = True
                    to_print += '\nNode {} Has wrong finger table {} value. Expected: {}\tActual: {}'.format(curr_ID, ind, e_ft_i, ft_i)

            # Check if successor list is correct
            # The + 2 when indexing is because succ_list starts two the right with our implementation.
            for ind, succ in enumerate(curr_node.var['succ_list']):
                e_succ = self.nodeList[(node_ind + ind + 2) % len(self.nodeList)] 
                if not e_succ == succ:
                    fail_test = True
                    to_print += '\nNode {} has wrong successor list value at index {}. Expected: {}\tActual: {}'.format(curr_ID, ind, e_succ, succ)

            if fail_test:
                overall_fail_test = True
                print(to_print)
        if not overall_fail_test:
            print('No errors in Chord Ring')

    '''
    Return key distribution function
    '''
    def return_key_distribution(self):
        ''' scans through nodes and returns list of number of keys 
        such that list is as long as nodeList'''
        keyFreqList = []

        for node_name in self.nodeList:
            keyFreqList.append(self.nodeDict[node_name].count_keys())
        assert len(keyFreqList) == len(self.nodeList), "error, keyFreqList list is len {}, nodeList is len {}".format(len(keyFreqList),
                                                                                                                    len(self.nodeList))    
        return keyFreqList

    '''
    To String function
    '''
    def __str__(self):
        to_print = ""
        for ID in self.nodeList:
            node = self.nodeDict[ID]
            to_print += 'Node {}\n'.format(ID) + '-'*40 + '\n'
            #for RPC_message in node.incoming_RPCs:
            #    to_print += self.RPC_to_string(RPC_message)
            #to_print += 'RPC Queue: {}\n'.format(node.incoming_RPCs)
            to_print += 'Successor List: {}\n'.format([node.var['finger_table_0']] + node.var['succ_list'])
            to_print += 'Predecessor: {}\n'.format(node.var['pred_ID'])
            for i in range(c.ring_size):
                var_name = 'finger_table_{}'.format(i)
                to_print += 'Finger Table Entry {}: {}\n'.format(i, node.var[var_name])
            to_print += 'Stored keys: {}\n\n'.format(node.var['storage'].keys())
        return to_print

    def RPC_to_string(self, RPC_message):
        RPC_type, RPC = RPC_message
        if RPC_type == 'value':
            return ''
        else:
            func_name, kwargs = RPC
            to_print = '{}: '.format(func_name)
            for k in kwargs.keys():
                if k == 'nodeDict': continue
                to_print += '{}-{} '.format(k, kwargs[k])
            to_print += '\n'
            return to_print
