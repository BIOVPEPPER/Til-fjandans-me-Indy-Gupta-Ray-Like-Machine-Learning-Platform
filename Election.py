import hashlib
import socket
import sys
import threading
import time
import os
Master_Num = 0
ML_Num = 0
TIMEOUT = 3
FORMAT = "utf-8"
RECENT = ['' for i in range(10)]
with open('../config.txt') as f:
    line = f.readlines()[0]
    MACHINENUM, SELF_IP = int(line.split(" ")[0].strip()), line.split(" ")[1].strip()

getmem_Receive = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
getmem_Receive.bind(('0.0.0.0',6012))
inform_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
inform_UDP.bind(('0.0.0.0', 6027))
OK_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
OK_UDP.bind(('0.0.0.0',6014))


'''Function for receiving failure information from membership service. 
    If failed node is coordinator, start electing'''
def Fail_Detect():
    global RECENT
    global ML_Num
    Failure_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    Failure_UDP.bind(("0.0.0.0",6011))
    while True:
        Fail_msg, _= Failure_UDP.recvfrom(1024)
        Fail_msg = Fail_msg.decode(FORMAT)
        if Fail_msg in RECENT:
            continue
        RECENT.pop(0)
        RECENT.append(Fail_msg)
        Fail_num = int(Fail_msg.split(' ')[1].split(',')[0][1])
        getmem_Receive.sendto(b'GETMEM 6012', ('127.0.0.1', 5004))
        #Also send this to the CoordinatorWatcher
        mem_list, _ = getmem_Receive.recvfrom(1024)
        mem_list = mem_list.decode(FORMAT)
        mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.split(',')}
        if Fail_num == ML_Num and MACHINENUM == ML_Num + 1:
            ML_Num = (ML_Num + 1)%len(mem_list)
            Failure_UDP.sendto(b'AC',('127.0.0.1',8013))
            for i in mem_list.keys():
                Failure_UDP.sendto(('MLC'+" "+str(ML_Num)).encode('utf-8'),(mem_list[i][0],8014))

        if Fail_num == Master_Num:
            candidates = [i for i in mem_list.keys() if int(i) > MACHINENUM]
            if candidates == []:
                if Master_Num != MACHINENUM:
                    print('Self electing: '+Fail_msg)
                    self_election(mem_list,Fail_msg)
                else:
                    continue
            else:
                for i in candidates:
                    Election_send(i,mem_list,Fail_msg)
                deadline = time.time()+TIMEOUT
                msg_OK, _ = None,None
                while time.time() < deadline:
                    msg_OK,_= OK_UDP.recvfrom(1024)
                if msg_OK == None: 
                    if Master_Num != MACHINENUM:
                        print('Self electing: '+Fail_msg)
                        self_election(mem_list,Fail_msg)    
                    else:
                        continue  
        else:
            if str(Master_Num) in mem_list:
                Failure_UDP.sendto(Fail_msg.encode(FORMAT),(mem_list[str(Master_Num)][0],6017))
            if str(ML_Num) in mem_list:
                Failure_UDP.sendto(Fail_msg.encode(FORMAT),(mem_list[str(ML_Num)][0],8015))
            if ML_Num == 0:
                Failure_UDP.sendto(Fail_msg.encode(FORMAT),(mem_list[str(ML_Num+1)][0],8015))


'''Function for receiving election messages from other nodes'''
def Election_receive():
    global RECENT
    Rec_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    Rec_UDP.bind(('0.0.0.0',6013))
    while True:
        msg,_ = Rec_UDP.recvfrom(1024)
        msg = msg.decode(FORMAT)
        Fail_msg = ' '.join(msg.split(' ')[1:])
        if Fail_msg in RECENT:
            continue
        RECENT.pop(0)
        RECENT.append(Fail_msg)
        if msg.startswith('E'):
            Rec_UDP.sendto(b'OK',(_[0],6014))
            getmem_Receive.sendto(b'GETMEM 6012', ('127.0.0.1', 5004))
            localmem_list, _ = getmem_Receive.recvfrom(1024)
            localmem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in localmem_list.decode('utf-8').split(',')}
            follow_candidates = [i for i in localmem_list.keys() if int(i) > MACHINENUM]

            if follow_candidates == [] :
                if Master_Num != MACHINENUM:
                    print('Self electing: '+Fail_msg)
                    self_election(localmem_list,Fail_msg)
                else:
                    continue
            else:
                for i in follow_candidates:
                    Election_send(i,localmem_list,Fail_msg)
                msg_OK, _ = None,None
                deadline = time.time()+TIMEOUT
                while time.time() < deadline:
                    msg_OK,_= OK_UDP.recvfrom(1024)
                if msg_OK == None:
                    if Master_Num != MACHINENUM:
                        print('Self electing: '+Fail_msg)
                        self_election(localmem_list,Fail_msg)    
                    else:
                        continue

'''Function for sending election messages from other nodes'''
def Election_send(candidate,mem_list,Fail_msg):
    candidate_IP = mem_list[candidate][0]
    Send_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    Send_UDP.sendto((('E '+Fail_msg).encode(FORMAT)),(candidate_IP,6013))

'''Function for receiving new master decision from other ndoes'''
def Receive_new_master_msg():
    global Master_Num
    RE_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    RE_UDP.bind(('0.0.0.0',6015))
    while True:
        master_msg, _ = RE_UDP.recvfrom(1024)
        master_msg = master_msg.decode(FORMAT)
        new_num = int(master_msg.split(' ')[-1])
        Master_Num = new_num 

'''Function for receiving notice of new ML master'''
def Receive_new_ML_msg():
    global ML_Num
    REML_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    REML_UDP.bind(('0.0.0.0',8014))
    while True:
        ML_msg, _ = REML_UDP.recvfrom(1024)
        ML_msg = ML_msg.decode(FORMAT)
        new_num_ML = int(ML_msg.split(' ')[-1])
        print('New ML Num received' + str(new_num_ML))
        ML_Num = new_num_ML
    
'''Function for electing self as new master'''
def self_election(mem_list,Fail_msg):
    #Elect itself as the Coordinator
    global Master_Num
    test_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    Master_Num = MACHINENUM
    try:
        #test_UDP.bind(('0.0.0.0', 6004))
        cmd = 'python3 Coordinator.py &'
        os.system(cmd)
    except:
        return
    
    Inform_Nums = [i for i in mem_list.keys() if int(i) < MACHINENUM]
    data, addr = inform_UDP.recvfrom(1024)
    if data == b'SUCCESS':
        print('Coordinator boosting succeed!')
    else:
        print('Coordinator boosting failed!')
        return
    print('self election sending to coordinator:')
    inform_UDP.sendto(Fail_msg.encode(FORMAT),('0.0.0.0',6017))
    inform_UDP.sendto(b'ELEC',('0.0.0.0',5007))
    for i in Inform_Nums:
        if i != MACHINENUM:
            inform_IP = mem_list[i][0]
            inform_UDP.sendto((('EF '+ str(MACHINENUM)).encode(FORMAT)),(inform_IP,6015))
        else:
            continue
    inform_UDP.close()

'''Helper for sending current master number to whoever request it.'''
def MasNumGetter():
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', 6019))

    while True:
        data, addr = sock.recvfrom(1024)
        if data.startswith(b'GETMAS'):
            port = data.decode(FORMAT).split(' ')[1]
            sock.sendto((str(Master_Num)).encode(FORMAT), ('127.0.0.1', int(port)))
        if data.startswith(b'GETMLMAS'):
            port = data.decode(FORMAT).split(' ')[1]
            sock.sendto((str(ML_Num)).encode(FORMAT), ('127.0.0.1', int(port)))


        

threading.Thread(target=Fail_Detect).start()
threading.Thread(target=Election_receive).start()
threading.Thread(target=Receive_new_master_msg).start()
threading.Thread(target=Receive_new_ML_msg).start()
threading.Thread(target=MasNumGetter).start()

    


    



            

        
                
