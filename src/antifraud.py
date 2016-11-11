from collections import defaultdict


#define class Graph, and some functions  

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
        
        elif g._graph[list(g._graph[node1])[0]] == {node1}: 
        	return "unverified"
        	
        elif g._graph[list(g._graph[node2])[0]] == {node2}:
        	return "unverified" 

                 
        else: 
            while queue:            
                (vertex, path) = queue.pop(0)
                for next in self._graph[vertex] - set(path):
                    
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


# read-in previous recorded data 
batch_text = open('paymo_input/batch_payment.txt', 'r')
batch_data = batch_text.readlines()

batch_id2 = []
batch_id1 = []
m = len(batch_data)
for i in range(m):
    b = batch_data[i]
    if str(b).startswith('2016'):
        u = str(batch_data[i]).strip().replace(',', '').split()
        batch_id1.append(int(u[2]))
        batch_id2.append(int(u[3]))

batch_z = zip(batch_id1, batch_id2)
batch = list(batch_z)


#convert previous data to graph 
g = Graph(batch, directed = False)

#read-in streaming data 

stream_text = open('paymo_input/stream_payment.txt', 'r')
stream_data = stream_text.readlines()

# clear previous recorded data in the output files 
f1 = open('paymo_output/output1.txt','w').close()
f2 = open('paymo_output/output2.txt','w').close()
f3 = open('paymo_output/output3.txt','w').close()

f1 = open('paymo_output/output1.txt','a')
f2 = open('paymo_output/output2.txt','a')
f3 = open('paymo_output/output3.txt','a')



import timeit 

start = timeit.default_timer()
# determine if the transaction is 'trusted' or 'unverified' line by line 
n = len(stream_data)
for i in range(n)[:5]:
    e = stream_data[i]
    if str(e).startswith('2016'):
        d = str(e).strip().replace(',', '').split()
        stream_id1 = int(d[2])
        stream_id2 = int(d[3])
        if g.had_transaction_before(stream_id1, stream_id2):
            f1.write('trusted\n')
            f2.write('trusted\n')
            f3.write('trusted\n')
        elif g.f_of_f(stream_id1, stream_id2):
            f1.write('unverified\n')
            f2.write('trusted\n')
            f3.write('trusted\n')
        elif g.fourth_connections(stream_id1, stream_id2) == 'trusted':
            f1.write('unverified\n')
            f2.write('unverified\n')
            f3.write('trusted\n')
        else: 
            f1.write('unverified\n')
            f2.write('unverified\n')
            f3.write('unverified\n')

stop = timeit.default_timer()
print(stop - start)
f1.close()
f2.close()
f3.close()




