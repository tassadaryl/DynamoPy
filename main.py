from collections import defaultdict
import threading
from hashlib import md5
import time
import random

LATENCY_LOWER_BOUND = 100
LATENCY_UPPER_BOUND = 1000

def latency():
    return float(random.randint(LATENCY_LOWER_BOUND, LATENCY_UPPER_BOUND)) / 1000.

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
    # def __repr__(self) -> str:
    #     return "my id:{0}".format(self.my_node_id)

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
        time.sleep(latency())
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

        #preference list   {key : preference node list}
        self.preference_list = defaultdict(list)
    
    def _Dget(self, key):
        if key not in self.preference_list:
            return "Key not in kv_store"
        
        node_coord = self.preference_list[key][0]
        node_rest1, node_rest2 = self.preference_list[key][1], self.preference_list[key][2]

        kv_coord, clock_coord = node_coord._Get(key)
        return_R = []

        t1 = threading.Thread(target=self._ThreadGet, args=(node_rest1, key, return_R))
        t2 = threading.Thread(target=self._ThreadGet, args=(node_rest2, key, return_R))
        t1.start()
        t2.start()
        while 1:
            if len(return_R) >= self.R - 1:
                break
        
        val_uptodate = kv_coord
        clock_uptodate = clock_coord
        reconcile_flag = False

        for node, kv, clock in return_R:
            if val_uptodate != kv:
                reconcile_flag = True
                compare_value = self._CompareClock(clock_uptodate, clock)
                if compare_value == 1:
                    pass
                elif compare_value == 2:
                    val_uptodate = kv
                    clock_uptodate = clock
                else:
                    #simplied version, TODO: choose which one to update/merge
                    pass

        # if reconcile is needed
        if reconcile_flag:
            #update it self
            self._Dreconcile(node_coord, key, val_uptodate, clock_uptodate)
            #update others
            treconcile1 = threading.Thread(target=self._Dreconcile, args=(node_rest1, key, val_uptodate, clock_uptodate))
            treconcile2 = threading.Thread(target=self._Dreconcile, args=(node_rest2, key, val_uptodate, clock_uptodate))
            treconcile1.start()
            treconcile2.start()
            print("reconciled!")

        return val_uptodate

    def _Dput(self, key, val):
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
        while 1:
            if len(return_W) >= self.W - 1:
                break
        
        return "Dynamo put success"

    def _Dreconcile(self, node:Node, key, val, clock):
        node._Reconcile(key, val, clock)
        return

    def _ThreadGet(self, node:Node, key, return_R):
        kv, clock = node._Get(key)
        return_R.append([node, kv, clock])
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

        print("key:{}, preference list:{}".format(key, self.preference_list[key]))
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
        for i in range(5):
            print(self.node_list[i])




def dput(dynamo: Dynamo, key, val):
    dynamo._Dput(key, val)


def dget(dynamo: Dynamo, key):
    dynamo._Dget(key)

if  __name__ == "__main__":

    dynamo = Dynamo()
    thread_pool_put = []
    for i in range(10):
        # dynamo._Dput(0, i)
        thread_pool_put.append(threading.Thread(target=dput, args=(dynamo, i, i)))

    for i in range(10):
        thread_pool_put[i].start()
    for i in range(10):
        thread_pool_put[i].join()

    thread_pool_get = []
    for i in range(10):
        thread_pool_get.append(threading.Thread(target=dget, args=(dynamo, i)))

    for i in range(10):
        thread_pool_get[i].start()
    for i in range(10):
        thread_pool_get[i].join()


    
    


    dynamo._Inspect()



