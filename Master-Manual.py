__author__ = 'Jordan'
from multiprocessing import Process,Pipe
import Server
import Client
import time

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
    cls[cid][1].recv()


def crashServer(sid,svs):
    svs[sid][0].terminate()
    svs[sid][2] = False


def restartServer(sid,svs,cls):
    num = sid
    here, there = Pipe()
    prss = Process(target=Server.server,args=(num,there,len(svs),len(cls),True))
    prss.start()
    svs[num] = [prss, here, True]
    time.sleep(.05)


def allClear(svs):
    print('sent allClear')
    for key in svs:
        if svs[key][2]:
            svs[key][1].send([1])
    for key in svs:
        if svs[key][2]:
            svs[key][1].recv()
    print('all clear')
    time.sleep(.05)


def timeBombLeader(svs,nummsgs):
    print('timeBombLeader',nummsgs)
    for key in svs:
        if svs[key][2]:
            svs[key][1].send([2,nummsgs])
    savekey = -1
    for key in svs:
        if svs[key][2]:
            response = svs[key][1].recv()
            if response == 1:
                savekey = key
    print('leader is',savekey)
    svs[savekey][1].recv()
    svs[savekey][0].terminate()
    svs[savekey][2] = False


def printChatLog(cid,cls):
    cls[cid][1].send([3])
    chatlog = cls[cid][1].recv()
    print(chatlog)


def simple(servers,clients):
    start(3,1,servers,clients)

    allClear(servers)

    crashServer('1',servers)
    crashServer('2',servers)

    sendMessage('0','onlyMessage',clients)

    allClear(servers)

    printChatLog('0',clients)

    restartServer('1',servers,clients)

    allClear(servers)

    allClear(servers)

    printChatLog('0',clients)


def complex(servers,clients):
    start(3,3,servers,clients)

    allClear(servers)

    sendMessage('0','helloWorld',clients)
    sendMessage('1','helloWorld',clients)
    sendMessage('2','greetingsWorld',clients)

    allClear(servers)
    printChatLog('1',clients)

    print('forcibly crashing server 0')
    crashServer('0',servers)
    print('forcibly crashing server 1')
    crashServer('1',servers)

    restartServer('0',servers,clients)
    restartServer('1',servers,clients)

    allClear(servers)

    sendMessage('0','spaghetti',clients)

    allClear(servers)
    printChatLog('2',clients)

    print('forcibly crashing server 2')
    crashServer('2',servers)

    sendMessage('1','papyrus',clients)

    allClear(servers)
    printChatLog('0',clients)
    printChatLog('1',clients)
    printChatLog('2',clients)



if __name__ == '__main__':
    running = True
    servers = {}
    clients = {}
    #simple(servers,clients)
    complex(servers,clients)