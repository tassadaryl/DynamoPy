from collections import defaultdict
import threading
from hashlib import md5

class Node:
    def __init__(self, id):
        #this node's term
        self.my_term = 0
        # Vector clock is associated with every version of every key
        self.vector_clock = defaultdict(dict)
        #key-value store
        self.kv_store = defaultdict(int)
        #other server to communicate with
        self.other_node_id = [i for i in range(1, 4) if i!=id]
        #this node's id
        self.my_node_id = id

    def __repr__(self) -> str:
        return "my id:{0}, kv:{1}, clock:{2}".format(self.my_node_id, self.kv_store, self.vector_clock)

    # get Requestkey's value and vector clock
    def _Get(self, Request_key):
        print("term:{}, kv:{}, clock:{}".format(self.my_term, self.kv_store, self.vector_clock))
        return self.kv_store[Request_key], self.vector_clock[Request_key]


    def _Put_Coordinator(self, Request_key, Request_val, Request_context=None):
        self.my_term += 1
        local_clock_update = {self.my_node_id: self.my_term}
        self.vector_clock[Request_key].update(local_clock_update)
        self.kv_store[Request_key] = Request_val

        print("PutCoord: node:{} term:{}, kv:{}, clock:{}".format(self.my_node_id, self.my_term, self.kv_store, self.vector_clock))
        return self.vector_clock[Request_key]


    # Gurantee Write Operation
    def _Put(self, Request_key, Request_val, Request_context):
        #update coordinators's request
        self.kv_store[Request_key] = Request_val
        self.vector_clock[Request_key].update(Request_context)

        print("Put: node:{} term:{}, kv:{}, clock:{}".format(self.my_node_id, self.my_term, self.kv_store, self.vector_clock))
        return "success"

    # Reconcile when conflicting during Read Op
    def _Reconcile(self, Reconciled_key, Reconciled_val ,Reconciled_vector_clock):
        self.kv_store[Reconciled_key] = Reconciled_val
        self.vector_clock[Reconciled_key] = Reconciled_vector_clock

        print("term:{}, kv:{}, clock:{}".format(self.my_term, self.kv_store, self.vector_clock))
        return "success"


class Dynamo:
    def __init__(self, N=3, W=1, R=1) -> None:
        self.N = N
        self.W = W
        self.R = R
        self.node1 = Node(1)
        self.node2 = Node(2)
        self.node3 = Node(3)
        self.node_list = [self.node1, self.node2, self.node3]

        #preference list

    
    def _Dget(self, key):
        return 
    
    def _Dput(self, key, val):
        return

    def _Dreconcile(self, list_of_vector_clock):
        return

    def _Dhash(self, key, mod):
        string = str(key)
        encoded_string = string.encode()
        hashed_string = md5(encoded_string)
        hexi = hashed_string.hexdigest()
        deci = int(hexi, 16)
        hash_value = deci % mod

        return hash_value














def put(node_a:Node, node_b:Node, key, val):
    node_a_clock = node_a._Put_Coordinator(key, val)
    node_b._Put(key, val, node_a_clock)


if  __name__ == "__main__":
    
    key1, val1 = 10, 20
    key2, val2 = 100, 200

    node1 = Node(1)
    node2 = Node(2)
    node3 = Node(3)
    
    t1 = threading.Thread(target=put, args=(node1, node2, key1, val1)) 
    t2 = threading.Thread(target=put, args=(node2, node1, key2, val2))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    print(node1)
    print(node2)





