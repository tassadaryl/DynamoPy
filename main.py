from collections import defaultdict
import threading
from hashlib import md5
import time
import random
import numpy as np

LATENCY_LOWER_BOUND = 0
LATENCY_UPPER_BOUND = 300
DROP_RATE = 0.0
RANDOM_LIST = [i for i in range(1, 101)]
MU_WRITE, SIGMA_WRITE = 0.025, 0.02
MU_READ, SIGMA_READ = 0.1, 0.02


#return latency sampled from a guassian distribution
def _Latency(mu=0.1, sigma=0.02):
    sample = np.random.normal(mu, sigma, 1)[0]
    return max(0, sample)

def _Drop():
    RANDOM_NUM = random.choice(RANDOM_LIST)
    if RANDOM_NUM <= int(100*DROP_RATE):
        return True
    return False

class Node:
    def __init__(self, id):
        #this node's term
        self.my_term = 0
        # Vector clock is associated with every version of every key
        self.vector_clock = defaultdict(dict)
        #key-value store
        self.kv_store = defaultdict(int)
        #this node's id
        self.my_node_id = id
        #lock
        self.lock = threading.Lock()

    def __repr__(self) -> str:
        return "Node id:{0}, kv:{1}, clock:{2}".format(self.my_node_id, self.kv_store, self.vector_clock)


    def _Get_Coordinator(self, Request_key):

        return self.kv_store[Request_key], self.vector_clock[Request_key]

    # get Requestkey's value and vector clock
    def _Get(self, Request_key):
        time.sleep(_Latency(MU_READ, SIGMA_READ))
        if _Drop():
            print("DROP in getting key:{} on Node:{}".format(Request_key, self.my_node_id))
            return None, None
        else:
            if Request_key not in self.kv_store:
                return None, None
            return self.kv_store[Request_key], self.vector_clock[Request_key]


    def _Put_Coordinator(self, Request_key, Request_val, Request_context=None):
        self.my_term += 1
        local_clock_update = {self.my_node_id: self.my_term}
        self.vector_clock[Request_key].update(local_clock_update)
        self.kv_store[Request_key] = Request_val

        return self.vector_clock[Request_key]


    # Gurantee Write Operation
    def _Put(self, Request_key, Request_val, Request_context):
        time.sleep(_Latency(MU_WRITE, SIGMA_WRITE))
        if _Drop():
            print("DROP in putting key:{} of val:{} on Node:{}".format(Request_key, Request_val, self.my_node_id))
            pass
        else:
            #update coordinators's request
            self.kv_store[Request_key] = Request_val
            self.vector_clock[Request_key].update(Request_context)

            return "success"

    def _Reconcile_Coordinator(self, Reconciled_key, Reconciled_val ,Reconciled_vector_clock):
        return

    # Reconcile when conflicting during Read Op
    def _Reconcile(self, Reconciled_key, Reconciled_val ,Reconciled_vector_clock):
        # time.sleep(_Latency())
        self.kv_store[Reconciled_key] = Reconciled_val
        self.vector_clock[Reconciled_key] = Reconciled_vector_clock

        return "success"


class Dynamo:
    def __init__(self, N=3, W=2, R=2) -> None:
        self.N = N
        self.W = W
        self.R = R
        #total number of nodes in dynamo
        self.M = 5
        self.node0 = Node(0)
        self.node1 = Node(1)
        self.node2 = Node(2)
        self.node3 = Node(3)
        self.node4 = Node(4)
        self.node_list = [self.node0, self.node1, self.node2, self.node3, self.node4]

        self.record = []

        #preference list   {key : preference node list}
        self.preference_list = defaultdict(list)
    
    def _Dget(self, key):
        if key not in self.preference_list:
            return "Key not in kv_store"
        
        print("Querying key:{}".format(key))
        
        node_coord = self.preference_list[key][0]
        node_rest1, node_rest2 = self.preference_list[key][1], self.preference_list[key][2]

        val_coord, clock_coord = node_coord._Get_Coordinator(key)
        return_R = []

        t1 = threading.Thread(target=self._ThreadGet, args=(node_rest1, key, return_R))
        t2 = threading.Thread(target=self._ThreadGet, args=(node_rest2, key, return_R))
        t1.start()
        t2.start()

        timeout_flag = False
        timeout = time.time() + 1
        while 1:
            if len(return_R) >= self.R - 1 or time.time() > timeout:
                if time.time() > timeout:
                    timeout_flag = True
                    print("TIMEOUT on getting key:{}".format(key))
                break
        
        val_uptodate = val_coord
        clock_uptodate = clock_coord
        node_uptodate = node_coord
        reconcile_flag = False

        node_status = []
        for node in self.preference_list[key]:
            node_status.append(node.kv_store[key] if key in node.kv_store else -1)
        print(node_status)

        for node, val, clock in return_R:
            if val_uptodate != val:
                reconcile_flag = True
                compare_value = self._CompareClock(clock_uptodate, clock)
                if compare_value == 1:
                    pass
                elif compare_value == 2:
                    val_uptodate = val
                    clock_uptodate = clock
                    node_uptodate = node
                else:
                    #simplied version, TODO: choose which one to update/merge
                    pass

        t1.join()
        t2.join()

        # if reconcile is needed
        if reconcile_flag :
            node_uptodate.my_term += 1
            clock_uptodate[node_uptodate.my_node_id] = node_uptodate.my_term
            #update itself
            self._Dreconcile(node_coord, key, val_uptodate, clock_uptodate)
            #update others
            treconcile1 = threading.Thread(target=self._Dreconcile, args=(node_rest1, key, val_uptodate, clock_uptodate))
            treconcile2 = threading.Thread(target=self._Dreconcile, args=(node_rest2, key, val_uptodate, clock_uptodate))
            treconcile1.start()
            treconcile2.start()
            print("Reconciled on key:{} of val:{}".format(key, val_uptodate))

        if not timeout_flag:
            print("Get key:{}, val:{}".format(key, val_uptodate))
        
        if not timeout_flag and not reconcile_flag and node_status[0] == node_status[1] and node_status[0] == node_status[2]:
            self.record.append(1)
        else:
            self.record.append(0)

        return val_uptodate

    def _Dput(self, key, val):
        print("Put key:{}, val:{}".format(key, val))
        if key not in self.preference_list:
            self._InitializeKey(key)
        
        node_coord = self.preference_list[key][0]
        node_rest1, node_rest2 = self.preference_list[key][1], self.preference_list[key][2]

        #Coordinator put key val:
        clock_coord = node_coord._Put_Coordinator(key, val)
        #send to other nodes
        return_W = []
        t1 = threading.Thread(target=self._ThreadPut, args=(node_rest1, key, val, clock_coord, return_W))
        t2 = threading.Thread(target=self._ThreadPut, args=(node_rest2, key, val, clock_coord, return_W))
        t1.start()
        t2.start()
        timeout = time.time() + 1
        while 1:
            if len(return_W) >= self.W - 1 or time.time() > timeout:
                break

        # t1.join()
        # t2.join()

        return "Dynamo put success"

    def _Dreconcile(self, node:Node, key, val, clock):
        node._Reconcile(key, val, clock)
        return

    def _ThreadGet(self, node:Node, key, return_R):
        val, clock = node._Get(key)
        if val == None:
            return
        return_R.append([node, val, clock])
        return

    def _ThreadPut(self, node:Node, key, val, clock, return_W):
        if node._Put(key, val, clock):
            return_W.append(node)
        return

    def _CompareClock(self, clock1:dict, clock2:dict):
        # 0:concurrent, 1:clock1 lead  2:clock2 lead
        if len(clock1) != len(clock2):
            return 0

        sharedKeys = set(clock1.keys()).intersection(clock2.keys())
        if len(clock1) != len(sharedKeys):
            return 0

        if all(clock1[key] == clock2[key] for key in sharedKeys):
            return 1
        elif any (clock1[key] < clock2[key] for key in sharedKeys) and any (clock1[key] > clock2[key] for key in sharedKeys):
            return 0
        elif any (clock1[key] < clock2[key] for key in sharedKeys):
            return 2
        else:
            return 1

    def _InitializeKey(self, key):
        #generate key's hash in [0, M)
        key_hash = self._Dhash(key, self.M)
        #store key's preference list
        for i in range(key_hash, key_hash + self.N):
            self.preference_list[key].append(self.node_list[i % self.M])

        # print("key:{}, preference list:{}".format(key, self.preference_list[key]))
        return

    def _Dhash(self, key, mod):
        #hash the key 
        #int -> str -> encoding -> md5hash -> heximal -> decimal -> mod value
        string = str(key)
        encoded_string = string.encode()
        hashed_string = md5(encoded_string)
        hexi = hashed_string.hexdigest()
        deci = int(hexi, 16)
        hash_value = deci % mod

        return hash_value

    def _Inspect(self):
        print("-------------Inspect--------------")
        for i in range(5):
            print(self.node_list[i])
        print("-------------Inspect--------------")

    def _Pconsistent(self):
        print(len([i for i in self.record if i == 1]) / 100.)
        return
        


def dput(dynamo: Dynamo, key, val):
    dynamo._Dput(key, val)


def dget(dynamo: Dynamo, key):
    time.sleep(0.1)
    dynamo._Dget(key)

if  __name__ == "__main__":

    dynamo = Dynamo()
    for i in range(100):
        t1 = threading.Thread(target=dput, args=(dynamo, 1, i))
        t2 = threading.Thread(target=dget, args=(dynamo, 1))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    dynamo._Pconsistent()



