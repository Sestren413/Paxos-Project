import socket
import select


def client(num,master,nums):
    #print('client',num,'created')

    # Connect to all Server Replicants
    replicants = {}
    connecting = True
    while connecting:
        for i in range(int(nums)):
            port = 55000 + i
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                if not i in replicants or replicants[i] == 'error':
                    c.connect(('localhost',port))
                    replicants[i] = c
                    c.send(int(num).to_bytes(1,'big'))
            except ConnectionError or KeyError:
                replicants[i] = 'error'
        connecting = False
        for i in range(int(nums)):
            if replicants[i] == 'error':
                connecting = True
    #print('client',num,'connected all servers')

    # Make no assumptions RE: Leader
    # Wait until told
    current_leader = -1
    live_leader = False

    # Flag for the allClear
    #clear_respond = False

    unique_id = 0
    unsequenced_messages = []
    unsent_messages = []
    incoming_messages = []
    chatlog = {}

    chatlog_flag = False

    # Main Loop
    while True:

        test = master.poll()
        if test:
            command = master.recv()
            if command[0] == 0:  # sendMessage
                print('client',num,'told to send message',command[1])
                messagebinary = command[1].encode('utf8')
                uniqueid = unique_id.to_bytes(1,'big')
                uniquemessage = uniqueid + messagebinary
                unsequenced_messages.append(uniquemessage)
                unsent_messages.append(uniquemessage)
                unique_id += 1
                master.send(0)
            #elif command[0] == 1:  # allClear
                #clear_respond = True
            elif command[0] == 3:  #printChatLog
                print('client',num,'told to print chatlog')
                chatlog_flag = True
            else:
                print('client',num,'unknown command',command)

        # Send unsent messages
        if live_leader:
            for message in unsent_messages:
                messageid = int(num).to_bytes(1,'big')
                messagecontent = messageid+message
                length = len(messagecontent).to_bytes(1,'big')
                chatmessage = length + messagecontent
                try:
                    replicants[current_leader].send(chatmessage)
                except:
                    replicants[current_leader].close()
                    replicants[current_leader] = 'dead'
                    live_leader = False
                    unsent_messages = unsequenced_messages.copy()
                    #print('client',num,'lost current leader during message send')
                    break
                else:
                    unsent_messages.remove(message)
                    #print('client',num,'sent',chatmessage,'to',current_leader)

        # Check incoming messages
        inlist = []
        [inlist.extend([k]) for k in replicants.values()]
        inlist[:]=[x for x in inlist if x != 'dead']
        if inlist:
            ready_to_read, b, c = select.select(inlist,[],[],0.01)
            for sock in ready_to_read:
                try:
                    msg = sock.recv(1)
                    if msg == b'':
                        for s in replicants:
                            if replicants[s] == sock:
                                replicants[s].close()
                                replicants[s] = 'dead'
                                if s == current_leader:
                                    #print('client',num,'lost current leader')
                                    live_leader = False
                                    unsent_messages = unsequenced_messages.copy()
                    else:
                        leng = int.from_bytes(msg,'big')
                        msg = sock.recv(leng)
                        while len(msg)<leng:
                            msg += sock.recv(leng-len(msg))
                        incoming_messages.append(msg)
                except:
                    for s in replicants:
                        if replicants[s] == sock:
                            replicants[s].close()
                            replicants[s] = 'dead'
                            if s == current_leader:
                                #print('client',num,'lost current leader')
                                live_leader = False
                                unsent_messages = unsequenced_messages.copy()

        # Act on incoming messages
        for message in incoming_messages:
            #print('client',num,'has message',message)
            mid = int.from_bytes(message[:1],'big')
            if mid == 0:  # update chatlog
                sequence_number = int.from_bytes(message[1:2],'big')
                sender_number = int.from_bytes(message[2:3],'big')
                uniquemessage = message[3:]
                chatmessage = message[4:].decode('utf8')
                if not sequence_number in chatlog:
                    chatlog[sequence_number] = [sender_number,chatmessage]
                    if sender_number == int(num):
                        if uniquemessage in unsequenced_messages:
                            unsequenced_messages.remove(uniquemessage)
                            #print('client',num,'remaining unseq',unsequenced_messages)
                else:
                    print('client',num,'received duplicate sequence number')
            elif mid == 1:  # new leader elected
                current_leader = int.from_bytes(message[1:],'big')
                live_leader = True
                #print('client',num,'learned of new leader')
            else:
                print('client',num,'received unknown message type')
            incoming_messages.remove(message)

        if chatlog_flag:
            master.send(chatlog)
            chatlog_flag = False

        for server in replicants:
            if replicants[server] == 'dead':
                c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    c.connect(('localhost',55000+server))
                except ConnectionRefusedError:
                    pass
                else:
                    replicants[server] = c
                    c.send(int(num).to_bytes(1,'big'))
                    print('client',num,'reconnected to server',server)