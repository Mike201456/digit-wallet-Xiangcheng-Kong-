
# coding: utf-8

# In[2]:

# read-in recorded data 
test_batch_text = open('/Users/xiangcheng/Documents/data insight/digital-wallet/insight_testsuite/tests/test02/paymo_input/batch_payment.txt', 'r')
test_batch_data = test_batch_text.readlines()



# In[3]:

test_batch_data


# In[4]:

test_batch_id2 = []
test_batch_id1 = []
c = 0
for i in range(len(test_batch_data))[1:]:
    if str(test_batch_data[i]).startswith('2016'):
        test_batch_id1.append(int(str(test_batch_data[i]).strip().replace(',', '').split()[2]))
        test_batch_id2.append(int(str(test_batch_data[i]).strip().replace(',', '').split()[3]))
    else:
        c+=1


# In[5]:

test_batch_z = zip(test_batch_id1, test_batch_id2)
test_batch = list(test_batch_z)


# In[6]:

test_batch


# In[7]:

from collections import defaultdict


class Graph(object):
    """ Graph data structure, undirected by default. """

    def __init__(self, connections, directed=False):
        self._graph = defaultdict(set)
        self._directed = directed
        self.add_connections(connections)

    def add_connections(self, connections):
        """ Add connections (list of tuple pairs) to graph """

        for node1, node2 in connections:
            self.add(node1, node2)

    def add(self, node1, node2):
        """ Add connection between node1 and node2 """

        self._graph[node1].add(node2)
        if not self._directed:
            self._graph[node2].add(node1)

    def remove(self, node):
        """ Remove all references to node """

        for n, cxns in self._graph.iteritems():
            try:
                cxns.remove(node)
            except KeyError:
                pass
        try:
            del self._graph[node]
        except KeyError:
            pass

    def had_transaction_before(self, node1, node2):
        """ Is node1 directly connected to node2 """

        return node1 in self._graph and node2 in self._graph and node2 in self._graph[node1] 
    
    def f_of_f(self, node1, node2):
    	""" Is node1 a friend of of a friend for node2"""
    	
    	return (self._graph[node1] & self._graph[node2]) 
    	
    def fourth_connections(self, node1, node2):
        """ Find any path between node1 and node2 (may not be shortest) """
        queue = [(node1, [node1])]
        
        if node1 not in self._graph:
            return "unverified"
        
        elif node2 not in self._graph:
            return "unverified"
        
        elif node2 in self._graph[node1]:
            return "trusted"
        
        elif (self._graph[node1] & self._graph[node2]):
            return "trusted"
        
        elif len(g._graph[node1]) >= 2:
        	if g._graph[list(g._graph[node1])[0]] == {node1}: 
        		return "unverified"
        	
        elif len(g._graph[node1]) >= 2:
        	if g._graph[list(g._graph[node2])[0]] == {node2}:
        		return "unverified" 

                 
        else: 
            while queue:            
                (vertex, path) = queue.pop(0)
                for next in self._graph[vertex] - set(path):
                    #print(path+[next])
                    #print(next)
                    if len(path +[next]) <=5:
                        if next == node2:
                            return "trusted"
                            break 
                        else:
                            queue.append((next, path + [next]))
                        
                    else: 
                        return "unverified"
                        break 
                    
                
        
    def __str__(self):
        return '{}({})'.format(self.__class__.__name__, dict(self._graph))





# In[8]:

g = Graph(test_batch, directed = False)


# In[10]:

# read in streaming data 


test_stream_text = open('/Users/xiangcheng/Documents/data insight/digital-wallet/insight_testsuite/tests/test02/paymo_input/stream_payment.txt', 'r')
test_stream_data = test_stream_text.readlines()
test_stream_data



from datetime import datetime
import numpy as np



lst05 = []

lst06 = []
lst07 = []
start=datetime.now()
n = len(test_stream_data)
f1 = open('/Users/xiangcheng/Documents/data insight/digital-wallet/insight_testsuite/temp/paymo_output/output1.txt','a')
f2 = open('/Users/xiangcheng/Documents/data insight/digital-wallet/insight_testsuite/temp/paymo_output/output2.txt','a')
f3 = open('/Users/xiangcheng/Documents/data insight/digital-wallet/insight_testsuite/temp/paymo_output/output3.txt','a')



for i in np.arange(n):
    
    if str(test_stream_data[i]).startswith('2016'):
        test_stream_id1 = int(str(test_stream_data[i]).strip().replace(',', '').split()[2])
        test_stream_id2 = int(str(test_stream_data[i]).strip().replace(',', '').split()[3])
        if g.had_transaction_before(test_stream_id1, test_stream_id2):
        	f1.write('trusted\n')
        else:
        	f1.write('unverified\n')
        	
        if g.f_of_f(test_stream_id1, test_stream_id2) or (g.had_transaction_before(test_stream_id1, test_stream_id2)):
        	f2.write('trusted\n')
        else:
        	f2.write('unverified\n')
        a = g.fourth_connections(test_stream_id1, test_stream_id2)
        	#f3.write(str())
        f3.write(a + '\n')
#print(datetime.now()-start)



