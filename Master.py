#! /usr/bin/python

from multiprocessing import Process,Pipe
import Server
import Client
import time
import fileinput
import string


def start(nserv,ncli,svs,cls):
    for i in range(nserv):
        num = str(i)
        here, there = Pipe()
        prss = Process(target=Server.server,args=(num,there,nserv,ncli,False))
        prss.start()
        svs[num] = [prss, here, True]
        time.sleep(.01)
    for i in range(ncli):
        num = str(i)
        here, there = Pipe()
        prss = Process(target=Client.client,args=(num,there,nserv))
        prss.start()
        cls[num] = [prss, here]
        time.sleep(.01)


def sendMessage(cid,message,cls):
    cls[cid][1].send([0,message])


if __name__ == "__main__":
    print('test string')
    nodes, clients, = {}, {}
    num_nodes, num_clients = 0, 0
    for line in fileinput.input():
        line = line.split();
        if line[0] == 'start':
            num_nodes = int(line[1])
            num_clients = int(line[2])
            """ start up the right number of nodes and clients, and store the 
                connections to them for sending further commands """
            start(num_nodes,num_clients,nodes,clients)
        if line[0] == 'sendMessage':
            client_id = int(line[1])
            message = string.join(line[2::])
            """ Instruct the client specified by client_index to send the message
                to the proper paxos node """
            sendMessage(client_id,message,clients)
        if line[0] == 'printChatLog':
            client_id = int(line[1])
            """ Print out the client specified by client_index's chat history
                in the format described on the handout """
        if line[0] == 'allClear':
            """ Ensure that this blocks until all messages that are going to 
                come to consensus in PAXOS do, and that all clients have heard
                of them """
        if line[0] == 'crashServer':
            node_index = int(line[1])
            """ Immediately crash the server specified by node_index """
        if line[0] == 'restartServer':
            node_index = int(line[1])
            """ Restart the server specified by node_index """
        if line[0] == 'timeBombLeader':
            num_messages = int(line[1])
            """ Instruct the leader to crash after sending the number of paxos
                related messages specified by num_messages """

