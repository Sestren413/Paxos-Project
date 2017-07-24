__author__ = 'Jordan'
import threading
import socket
import select
import pickle
from collections import Counter

def server(num,master,nums,numc,revived):
    print('server',num,'created')

    # Start quasi-independent acceptor
    acceptorthread = threading.Thread(target=acceptor,args=(num,nums))
    acceptorthread.start()

    threadsclear = [False,False,False]
    insufficient_acceptors = [False]

    # Set up client-server comm thread
    commlock = threading.Lock()
    sslock = threading.Lock()
    prlock = threading.Lock()
    c_s_messages = []
    fullchatroom = {}
    old_chatroom = {}
    chatroom = {}
    view_num = 0
    current_leader = [-1]
    if not revived:
        current_leader[0] = 0
    liveservthread = threading.Thread(target=liveserver,args=(num,nums,view_num,current_leader,revived,fullchatroom,sslock,threadsclear))
    liveservthread.start()
    is_leader = [False]
    commthread = threading.Thread(target=clientcomm,args=(num,commlock,c_s_messages,chatroom,numc,is_leader,threadsclear))
    commthread.start()
    curr_seq_num = 0
    curr_pval_num = 0
    next_open_slot = 0

    timebomb_left = [-1,False]
    loopcount = 0

    proposals = []
    accepted_proposals = []
    rejected_proposals = []
    proposerthread = threading.Thread(target=proposer,args=(num,nums,proposals,accepted_proposals,rejected_proposals,prlock,timebomb_left,insufficient_acceptors))

    # Main Loop
    while True:

        if int(num) == current_leader[0]:
            if not is_leader[0]: # Just became new leader
                print('main thread BECAME LEADER',num)
                is_leader[0] = True
                loopcount = 0
                proposerthread.start()
                curr_pval_num = next_open_slot

        test = master.poll()
        if test:
            command = master.recv()
            if command[0] == 1:  # allClear
                threadsclear[0] = True
            elif command[0] == 2:  # timeBombLeader
                if is_leader[0]:
                    timebomb_left[0] = command[1]
                    master.send(1)
                else:
                    master.send(0)
            else:
                print('client',num,'unknown command',command)


        # Handle fullchatroom updates and convert them to nop free chatroom
        sslock.acquire()
        diff = set(fullchatroom.keys()) - set(old_chatroom.keys())
        diffsort = []
        if diff:
            loopcount = 0
            old_chatroom.update(fullchatroom)
            diffsort = sorted(diff)  # Ensure that accepted ordering is not scrambled on key retreival
        sslock.release()
        commlock.acquire()  # Sends new messages to clients
        for key in diffsort:
            next_open_slot += 1
            if fullchatroom[key] != 'nop':
                chatroom[curr_seq_num] = fullchatroom[key]
                curr_seq_num += 1
        commlock.release()


        # Handle messages from clients
        commlock.acquire()
        sslock.acquire()
        for message in c_s_messages:
            loopcount = 0
            print('server',num,'proposed',message)
            #print(fullchatroom,curr_pval_num)
            if message not in fullchatroom.values():  # Prevent duplication from leader deaths at weird times
                proposals.append([curr_pval_num,message])
            curr_pval_num += 1
            c_s_messages.remove(message)
        sslock.release()
        commlock.release()

        # Respond to allClear
        if threadsclear[0]:
            if threadsclear[1] and threadsclear[2] and loopcount > 3:  # c-s and s-s threads report all is well
                if not proposals and not accepted_proposals and not rejected_proposals:  # No outstanding proposals have yet to be dealt with
                    print(num,'clear from no proposals')
                    master.send(0)
                    threadsclear[0] = False
                    loopcount = 0
                elif insufficient_acceptors:  # No progress can be made because too many servers are down
                    print(num,'clear from no progress')
                    master.send(0)
                    threadsclear[0] = False
                    loopcount = 0
            elif not loopcount > 3:
                loopcount += 1
            else:
                pass

        # Handle message acceptance
        prlock.acquire()
        for proposal in accepted_proposals:
            loopcount = 0
            fullchatroom[proposal[0]]=proposal[1]
            print(proposal,'accepted')
            accepted_proposals.remove(proposal)
        for proposal in rejected_proposals:
            loopcount = 0
            proposal[0] = curr_pval_num
            proposals.append(proposal)
            curr_pval_num += 1
            rejected_proposals.remove(proposal)
        prlock.release()



def clientcomm(num,lock,msgs,chroom,numc,is_leader,threadsclear):
    print('c-s thread for server',num,'created')

    old_chroom = {}
    unplaced_requests = 0

    # Connect to clients
    myport = 55000 + int(num)
    servsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servsoc.bind(('localhost',myport))
    servsoc.listen(numc)
    clients = {}
    for i in range(int(numc)):
        s, addr = servsoc.accept()
        id = s.recv(1)
        conum = int.from_bytes(id,'big')
        clients[conum] = s

    was_leader = is_leader[0]
    if is_leader[0]:
        print('c-s thread BECAME LEADER',num)
        messagecontents = b'\x01' + int(num).to_bytes(1,'big')
        length = len(messagecontents).to_bytes(1,'big')
        leadmessage = length + messagecontents
        for cid in clients:
            clients[cid].send(leadmessage)

    clilist = []
    [clilist.extend([k]) for k in clients.values()]

    loopcount = 0

    # Main Loop
    while True:

        if is_leader[0]:
            if not was_leader:
                print('c-s thread BECAME LEADER',num)
                was_leader = True
                messagecontents = b'\x01' + int(num).to_bytes(1,'big')
                length = len(messagecontents).to_bytes(1,'big')
                leadmessage = length + messagecontents
                old_chroom.update(chroom)
                for cli in clients:
                    clients[cli].send(leadmessage)

        if threadsclear[0]:
            if loopcount > 3:
                threadsclear[1] = True
            else:
                threadsclear[1] = False
                loopcount += 1
                #print(num,'c-s loopcount',loopcount)
        else:
            threadsclear[1] = False

        # Check for client messages
        ready_to_read, b, c = select.select(clilist,[],[],0.05)
        newmsgs = []
        for sock in ready_to_read:  # Clients can't fail, no need for error handling
            loopcount = 0
            msg = sock.recv(1)
            leng = int.from_bytes(msg,'big')
            msg = sock.recv(leng)
            while len(msg)<leng:
                msg += sock.recv(leng-len(msg))
            newmsgs.append(msg)
            unplaced_requests += 1

        lock.acquire()
        msgs += newmsgs
        lock.release()

        # Send Chatroom updates to clients (if leader)
        if(is_leader[0]) and was_leader:
            lock.acquire()
            diff = set(chroom.keys()) - set(old_chroom.keys())
            # print('--=--\n',chroom.keys(),'\n',old_chroom.keys(),'\n',diff,'\n--=--')
            if diff:
                loopcount = 0
                old_chroom.update(chroom)
                print('c-s',num,'chatroom diff',diff)
            lock.release()
            for key in diff:
                senderid = int.from_bytes(chroom[key][:1],'big')
                messagecontents = b'\x00' + key.to_bytes(1,'big') + chroom[key]
                length = len(messagecontents).to_bytes(1,'big')
                logmessage = length + messagecontents
                for cid in clients:
                    #print('leader c-s sending message to client')
                    if cid != senderid:
                        clients[cid].send(logmessage)
                print('last c-s sent',senderid,logmessage)
                clients[senderid].send(logmessage)
                unplaced_requests -= 1


def liveserver(num,nums,view_num,current_leader,revived,chatroom,sslock,threadsclear):
    print('s-s thread',num,'created')

    myport = 55100 + int(num)
    servsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servsoc.bind(('localhost',myport))
    servsoc.listen(int(nums))
    #print(num,'s-s bound port')
    sslock.acquire()
    old_chatroom = {}
    connections_out = {}
    connecting = True
    while connecting:
        for i in range(int(nums)):
            #print(num,'s-s setting up outbound',i)
            if i != int(num):
                port = 55100 + i
                c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    if not i in connections_out or connections_out[i] == 'error':
                        c.connect(('localhost',port))
                        connections_out[i] = c
                        c.send(int(num).to_bytes(1,'big'))
                        if revived:
                            c.send(b'\x00')
                            messagecontent = pickle.dumps(chatroom)
                            length = len(messagecontent).to_bytes(2,'big')
                            chatroomcopy = length + messagecontent
                            c.send(chatroomcopy)
                except ConnectionError or KeyError:
                    connections_out[i] = 'error'
                    #print(num,'s-s hit error on outbound')
                else:
                    pass
                    #print('s-s thread',num,'sent outbound to',i)
            else:
                connections_out[i] = 'self'
        connecting = False
        for i in range(int(nums)):
            if connections_out[i] == 'error':
                if not revived:
                    connecting = True
                else:
                    connections_out[i] = 'dead'
    print('s-s thread',num,'outbound')

    connections_in = {}
    for i in range(int(nums)):
        if i != int(num) and connections_out[i] != 'dead':
            #print('s-s thread',num,'looking for inbound', i)
            s, addr = servsoc.accept()
            #print('s-s thread',num,'found inbound', i)
            id = s.recv(1)
            conum = int.from_bytes(id,'big')
            connections_in[conum] = s
            if revived:
                vnum = s.recv(1)
                view_num = max(view_num,int.from_bytes(vnum,'big'))
                crmlen = int.from_bytes(s.recv(2),'big')
                newcrmstr = s.recv(crmlen)
                newcrm = pickle.loads(newcrmstr)
                chatroom.update(newcrm)
                old_chatroom.update(chatroom)
                #print('reviving s-s',num,'updated chatroom',chatroom)
        else:
            if i == int(num):
                connections_in[i] = 'self'
            else:
                connections_in[i] = 'dead'
        #print('s-s thread',num,'finished inbound',i)
    print('s-s thread',num,'inbound')

    sslock.release()
    #print('s-s thread',num,'ready',connections_in,connections_out)
    print('s-s thread',num,'ready')
    if revived:
        print('s-s thread',num,'got view-num',view_num)
        current_leader[0] = view_num % int(nums)

    liveProcs = {}
    for i in range(int(nums)):
        liveProcs[i] = True

    lock = threading.Lock()
    reconnectinfo = [connections_in,connections_out,servsoc,num,view_num,liveProcs,lock]

    loopcount = [0]

    reconnectthread = threading.Thread(target=s_s_reconnectionthread,args=(reconnectinfo,chatroom,loopcount))
    reconnectthread.start()

    while True:

        if threadsclear[0]:
            if loopcount[0] > 3:
                threadsclear[2] = True
            else:
                threadsclear[2] = False
                loopcount[0] += 1
                #print(num,'c-s loopcount',loopcount)
        else:
            threadsclear[2] = False

        # Monitor channels to find dead servers
        inlist = []
        [inlist.extend([k]) for k in connections_in.values()]
        inlist[:]=[x for x in inlist if x != 'dead' and x != 'self']
        if inlist:
            lock.acquire()
            #print(num,lock,'main s-s')
            ready_to_read, b, c = select.select(inlist,[],[],0.05)
            #print(ready_to_read)
            for sock in ready_to_read:
                loopcount[0] = 0
                try:
                    leng = sock.recv(2)
                    if leng == b'':
                        print(num,'s-s connection closed--UNEXPECTED BEHAVIOR',sock)
                        while True:
                            pass  # Trap the thing so I can see what went wrong
                    else:
                        newcrmstr = sock.recv(int.from_bytes(leng,'big'))
                        newcrm = pickle.loads(newcrmstr)
                        sslock.acquire()
                        chatroom.update(newcrm)  # This is where chatroom updates arrive
                        old_chatroom.update(chatroom)  # Update diff-checking copy immediately so as to not re-send material
                        sslock.release()
                        print('s-s',num,'updated chatroom')
                except ConnectionError:
                    #print('connection error found',sock)
                    for s in connections_in:
                        if connections_in[s] == sock:
                            connections_out[s].close()
                            #print(num,s,'outbound closed')
                            connections_out[s] = 'dead'
                            connections_in[s].close()
                            #print(num,s,'inbound closed')
                            connections_in[s] = 'dead'
                            liveProcs[s] = False
                            print('server',num,'lost peer',s)
                            if s == current_leader[0]:
                                print('server',num,'lost current leader')
                                threadsclear[2] = False
                                while True:
                                    reconnectinfo[4] += 1
                                    potential_leader = reconnectinfo[4] % int(nums)
                                    if liveProcs[potential_leader]:
                                        current_leader[0] = potential_leader
                                        break
                                threadsclear[2] = True
                                print('server',num,'decided on',current_leader[0],'as new leader')
            lock.release()

        sslock.acquire()
        diff = set(chatroom.keys()) - set(old_chatroom.keys())
        if diff:  # Send updated fullchatroom
            loopcount[0] = 0
            old_chatroom.update(chatroom)
            messagecontent = pickle.dumps(chatroom)
            length = len(messagecontent).to_bytes(2,'big')
            chatroomcopy = length + messagecontent
            for server in connections_out:
                if connections_out[server] != 'dead' and connections_out[server] != 'self':
                    try:
                        connections_out[server].send(chatroomcopy)
                        print('s-s',num,'sent out updated chatroom copy')
                    except ConnectionError:
                        #print('connection error found',connections_out[server])
                        connections_out[server].close()
                        #print(num,server,'outbound closed')
                        connections_out[server] = 'dead'
                        connections_in[server].close()
                        #print(num,server,'inbound closed')
                        connections_in[server] = 'dead'
                        liveProcs[server] = False
                        print('server',num,'lost peer',server)  # Must be leader if sending out updates, no need to check for leader death
        sslock.release()


def s_s_reconnectionthread(reconnectinfo,chatroom,loopcount):
    while True:
        s, addr = reconnectinfo[2].accept()
        loopcount[0] = 0
        reconnectinfo[6].acquire()
        #print(reconnectinfo[3],reconnectinfo[6],'mini s-s')
        id = s.recv(1)
        conum = int.from_bytes(id,'big')
        reconnectinfo[0][conum] = s
        s.recv(1)  # Disregard view_num from fresh
        print('s-s',reconnectinfo[3],'found',conum,'sending viewnum',reconnectinfo[4])
        reconnectinfo[5][conum] = True
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect(('localhost',55100+conum))
        reconnectinfo[1][conum] = c
        c.send(int(reconnectinfo[3]).to_bytes(1,'big'))
        c.send(reconnectinfo[4].to_bytes(1,'big'))
        messagecontent = pickle.dumps(chatroom)
        length = len(messagecontent).to_bytes(2,'big')
        chatroomcopy = length + messagecontent
        c.send(chatroomcopy)
        crmlenb = s.recv(2)
        print('crmlenb',crmlenb)
        crmlen = int.from_bytes(crmlenb,'big')
        print('chatroombytes',s.recv(crmlen))  # Disregard chatroom from fresh
        print(reconnectinfo[3],'s-s dummy chatroom from revival arrived')
        reconnectinfo[6].release()
        #print(reconnectinfo[3],reconnectinfo[6])


def proposer(num,nums,proposals,accepted_proposals,rejected_proposals,prlock,tbleft,insacc):
    print('proposer thread',num,'created')
    acceptorpvalues = {}
    liveaccs = []
    for acc in range(int(nums)):
        acceptorpvalues[acc] = {}
        liveaccs.append(True)
    active = [False,int(num)]  # Ballot num and whether it is accepted

    while True:

        if not active[0]:
            acctest = 0
            for acc in liveaccs:
                if acc:
                    acctest += 1
            if acctest > int(nums)/2:
                print('found enough accs')
                insacc[0] = False
            else:
                print(liveaccs)
                insacc[0] = True

            scoutthread = threading.Thread(target=scout,args=(nums,active,acceptorpvalues,liveaccs,insacc))
            scoutthread.start()
            print(num,'scout shopping val',active[1])
            scoutthread.join()
            print(liveaccs,acctest,int(nums)/2)
            if active[0]:
                print('proposer',num,'active')
            else:
                if not insacc[0]:
                    active[1] += 10

        while not proposals and active[0]:
            pass  # To avoid grabbing the lock if there's nothing going on (busy-waiting bad blah blah its just one thread)

        if active[0]:
            prlock.acquire()
            for proposal in proposals:
                # Logic for handling preexisting entries in the desired slot
                #print('checking',proposal)
                preexisting_proposals = []
                empty_slots = 0
                for acc in range(int(nums)):
                    if proposal[0] in acceptorpvalues[acc]:
                        preexisting_proposals.append(acceptorpvalues[acc][proposal[0]])
                    else:
                        if liveaccs[acc]:
                            empty_slots += 1

                if empty_slots > int(nums)/2:
                    chosenprop = proposal
                    #print('proposer found a majority empty slots')
                else:
                    #print('proposer did not find a majority empty slots')
                    # Need to check if any existing proposal could have possibly achieved > int(nums)/2 accepts; if any of them could have been chosen
                    maxbal = -1
                    for prop in preexisting_proposals:
                        propbal = int.from_bytes(prop[:2],'big')
                        maxbal = max(maxbal,propbal)
                        if propbal == maxbal:
                            existingprop = prop[2:]
                    chosenprop = [proposal[0],existingprop]

                commandthread = threading.Thread(target=commander,args=(nums,active,chosenprop,insacc))
                commandthread.start()
                #print('commander proposing',active[1],chosenprop)
                commandthread.join()
                if active[0]:
                    print('proposal successful')
                    accepted_proposals.append(proposal)
                    proposals.remove(proposal)
                else:
                    if insacc[0]:
                        print('insufficient acceptors')
                        pass
                    else:
                        print('commander preempted')
                        rejected_proposals.append(proposal)
                        proposals.remove(proposal)
                        break
            prlock.release()




def scout(nums,active,apvs,liveaccs,insacc):
    accepted = 0
    highest = True
    for server in range(int(nums)):
        port = 55200 + server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('localhost',port))
        except ConnectionRefusedError:
            #print('acceptor missing')
            liveaccs[server] = False
        else:
            liveaccs[server] = True
            #print('acceptor found')
            messagecontent = b'\x00' + active[1].to_bytes(2,'big')
            length = len(messagecontent).to_bytes(2,'big')
            p1a = length + messagecontent
            sock.send(p1a)
            #print('p1a sent')
            pvlen = int.from_bytes(sock.recv(2),'big')
            pvstr = sock.recv(pvlen)
            #print('p1b received')
            returned_ballot = int.from_bytes(pvstr[:2],'big')
            pvals = pickle.loads(pvstr[2:])
            sock.close()
            #print('sock closed')
            apvs[server].update(pvals)
            if returned_ballot == active[1]:
                accepted += 1
            else:
                highest = False
    if highest and accepted > int(nums)/2:
        active[0] = True
    elif accepted > int(nums)/2:
        active[0] = False
        insacc[0] = False
    else:
        active[0] = False
        insacc[0] = True
    #print('scout exiting')
    return


def commander(nums,active,proposal,insacc):
    accepted = 0
    for server in range(int(nums)):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        port = 55200 + server
        try:
            sock.connect(('localhost',port))
        except ConnectionRefusedError:
            pass
        else:
            messagecontent = b'\x01' + active[1].to_bytes(2,'big') + proposal[0].to_bytes(1,'big') + proposal[1]
            length = len(messagecontent).to_bytes(2,'big')
            p1a = length + messagecontent
            #print('commander sending',p1a)
            sock.send(p1a)
            pvlen = int.from_bytes(sock.recv(2),'big')
            pvstr = sock.recv(pvlen)
            returned_ballot = int.from_bytes(pvstr[:2],'big')
            sock.close()
            if returned_ballot == active[1]:
                accepted += 1
            else:
                active[0] = False
                break
    if not accepted > int(nums)/2:
        insacc[0] = True
        active[0] = False
    return


def acceptor(num,nums):
    #print('acceptor',num,'created')
    ballot_num = -1
    pvalues = {}

    myport = 55200 + int(num)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost',myport))
    sock.listen(int(nums))

    while True:

        friend,addr = sock.accept()  # Block until someone starts talking
        leng = int.from_bytes(friend.recv(2),'big')
        message = friend.recv(leng)
        if message[:1] == b'\x00':
            new_ballot = int.from_bytes(message[1:],'big')
            if new_ballot > ballot_num:
                ballot_num = new_ballot
            #print('acceptor',num,'current balnum',ballot_num)
            pvb = pickle.dumps(pvalues)
            messagecontent = ballot_num.to_bytes(2,'big') + pvb
            length = len(messagecontent).to_bytes(2,'big')
            p1b = length + messagecontent
            friend.send(p1b)
            friend.close()
        elif message[:1] == b'\x01':
            #print(message)
            new_ballot = int.from_bytes(message[1:3],'big')
            if new_ballot > ballot_num:
                ballot_num = new_ballot
            #print(ballot_num)
            if new_ballot == ballot_num:  # add proposal to pvalues
                slot_num = int.from_bytes(message[3:4],'big')
                pvalues[slot_num] = ballot_num.to_bytes(2,'big') + message[4:]
            messagecontent = ballot_num.to_bytes(2,'big')
            length = len(messagecontent).to_bytes(2,'big')
            p2b = length + messagecontent
            friend.send(p2b)
            friend.close()
        else:
            print('acceptor',num,'received unknown message type')