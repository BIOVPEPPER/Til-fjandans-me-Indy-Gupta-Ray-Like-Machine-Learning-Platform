#from dataclasses import dataclass
#from heapq import heappush
import socket
import time
import threading
import queue
import random
import bisect
import logging
import os
#Previously defined, ID of the machine
JOINED = 0
LOSS_RATE = 0
MACHINENUM = 0
TIME_OUT = 4
#Define PRED_IP, SUCC_IP_1, SUCC_IP2, read from the IPadress file.
PRED_IP = ''
SUCC_IP_1 = ''
SUCC_IP_2 = ''
SELF_IP = ''
SELF_TS = str(time.time())
UPDATE_msgs = []
logging.basicConfig(filename="ActivityRecord.log", level=logging.INFO)
with open('../config.txt') as f:
    line = f.readlines()[0]
    MACHINENUM, SELF_IP = int(line.split(" ")[0].strip()), line.split(" ")[1].strip()
Membership = [(MACHINENUM, SELF_IP, SELF_TS)]

#Server
def server():
    global PRED_IP
    global SUCC_IP_1
    global SUCC_IP_2
    global SELF_IP
    global UPDATE_msgs
    ip_ADDRESS = "0.0.0.0"
    ip_PORT = 5001
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock.bind((ip_ADDRESS, ip_PORT))
    sock.settimeout(0.1)
    #Set a deadline for considering a machine as fail
    deadline1, deadline2,deadline3 = time.time() + TIME_OUT, time.time() + TIME_OUT, time.time() + TIME_OUT 
    while True:
        data, addr = None, None
        #Receive data
        try:
            data, addr = sock.recvfrom(1024)
        except:
            pass
        if data:
            #Respond PONG TO PING
            if data.startswith(b'PING'):
                msg = b"PONG"
                if random.randint(1,100) > LOSS_RATE:
                    sock.sendto(msg,(addr[0], 5001))
                events = data.decode('utf-8').split(';')
                if len(events) > 1:
                    for item in events[1:]:
                        proc_id = (int(item[2:].split(' ')[0]), item[2:].split(' ')[1], item[2:].split(' ')[2])
                        #Receive join request, add to the membership
                        if item[0] == 'J':
                            if proc_id not in Membership:
                                bisect.insort(Membership,proc_id)
                                UPDATE_msgs.append(item)
                                logging.info("FROM"+" "+str(addr[0])+":"+ str(item))
                        #Receive Delete request, remove from membership
                        if item[0] == 'D':
                            if proc_id in Membership:
                                if proc_id[1] == SELF_IP:
                                    os._exit(1)
                                Membership.remove(proc_id)
                                UPDATE_msgs.append(item)
                                logging.info("FROM"+" "+str(addr[0])+":"+ str(item))
                    PRE_index = Membership.index((MACHINENUM,SELF_IP,SELF_TS)) - 1
                    SUCC_index_1 = (Membership.index((MACHINENUM,SELF_IP,SELF_TS)) + 1 )%len(Membership) 
                    SUCC_index_2 = (Membership.index(((MACHINENUM,SELF_IP,SELF_TS))) + 2)%len(Membership)
                    #IF UPDATE the Membership, reset deadline as well.
                    if Membership[PRE_index][1] != PRED_IP:
                        deadline1 = time.time() + TIME_OUT
                    if Membership[SUCC_index_1][1] != SUCC_IP_1:
                        deadline2 = time.time() + TIME_OUT
                    if Membership[SUCC_index_2][1] != SUCC_IP_2:
                        deadline3 = time.time() + TIME_OUT 
                    PRED_IP = Membership[PRE_index][1]
                    SUCC_IP_1 = Membership[SUCC_index_1][1]
                    SUCC_IP_2 = Membership[SUCC_index_2][1]

                            
                        
            #If received the pong, reset the deadline
            elif data.startswith(b'PONG'):
                if addr[0] == PRED_IP:
                    deadline1 = time.time() + TIME_OUT
                if addr[0] == SUCC_IP_1:
                    deadline2 = time.time() + TIME_OUT
                if addr[0] == SUCC_IP_2:
                    deadline3 = time.time() + TIME_OUT
            


        #If any of the VM failed, delete them from the membership, and put it into messages, distribute to other VMs so they can update their membership.
        if time.time() > deadline1:
            #print("Pred died!")
            failed_proc = [item for item in Membership if item[1] == PRED_IP]
            if failed_proc:
                fmsg = 'D,'+' '.join([str(item) for item in failed_proc[0]])
                UPDATE_msgs.append(fmsg)
                PRED_IP = Membership[Membership.index(failed_proc[0])-1][1]
                if failed_proc[0][1] == SELF_IP:
                    os._exit(1)
                Membership.remove(failed_proc[0])
                logging.info("FROM"+" "+"SELF"+":"+ fmsg)
                deadline1 = time.time() + TIME_OUT
                warning_msg = 'F '+str(failed_proc[0])
                sock.sendto(warning_msg.encode('utf-8'), ('127.0.0.1', 6011))
                sock.sendto(warning_msg.encode('utf-8'), ('127.0.0.1', 6017))
                sock.sendto(warning_msg.encode('utf-8'), ('127.0.0.1', 8015))
        if time.time() > deadline2:
            #print("Succ1 died!")
            failed_proc = [item for item in Membership if item[1] == SUCC_IP_1]
            if failed_proc:
                fmsg = 'D,'+' '.join([str(item) for item in failed_proc[0]])
                UPDATE_msgs.append(fmsg)
                SUCC_IP_1 = Membership[(Membership.index(failed_proc[0])+1)%len(Membership)][1]
                if failed_proc[0][1] == SELF_IP:
                    os._exit(1)
                Membership.remove(failed_proc[0])
                logging.info("FROM"+" "+"SELF"+":"+ fmsg)
                deadline2 = time.time() + TIME_OUT
                warning_msg = 'F '+str(failed_proc[0])
                sock.sendto(warning_msg.encode('utf-8'), ('127.0.0.1', 6011))
                sock.sendto(warning_msg.encode('utf-8'), ('127.0.0.1', 6017))
                sock.sendto(warning_msg.encode('utf-8'), ('127.0.0.1', 8015))
        if time.time() > deadline3:
            #print("Succ2 Died!")
            failed_proc = [item for item in Membership if item[1] == SUCC_IP_2]
            if failed_proc:
                fmsg = 'D,'+' '.join([str(item) for item in failed_proc[0]])
                UPDATE_msgs.append(fmsg)
                SUCC_IP_2 = Membership[(Membership.index(failed_proc[0])+2)%len(Membership)][1]
                if failed_proc[0][1] == SELF_IP:
                    os._exit(1)
                Membership.remove(failed_proc[0])
                logging.info("FROM"+" "+"SELF"+":"+ fmsg)
                deadline3 = time.time() + TIME_OUT
                warning_msg = 'F '+str(failed_proc[0])
                sock.sendto(warning_msg.encode('utf-8'), ('127.0.0.1', 6011))
                sock.sendto(warning_msg.encode('utf-8'), ('127.0.0.1', 6017))
                sock.sendto(warning_msg.encode('utf-8'), ('127.0.0.1', 8015))

            
def client():
    global PRED_IP
    global SUCC_IP_1
    global SUCC_IP_2
    global UPDATE_msgs
    def RegPing(socketObj, ip, port, message):
        socketObj.sendto(message, (ip, port))



    sock = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    

    #Constantly ping the successors and predecessors, to get the ping.
    while True:
        if random.randint(1,100) > LOSS_RATE:
            RegPing(sock, PRED_IP, 5001, bytes('PING'+';'.join(['']+UPDATE_msgs), 'utf-8'))
        if random.randint(1,100) > LOSS_RATE:
            RegPing(sock, SUCC_IP_1, 5001, bytes('PING'+';'.join(['']+UPDATE_msgs),'utf-8'))
        if random.randint(1,100) > LOSS_RATE:
            RegPing(sock, SUCC_IP_2, 5001, bytes('PING'+';'.join(['']+UPDATE_msgs),'utf-8'))
        UPDATE_msgs = []
        time.sleep(1)


#Only avaliable on VM1, process the join request
def introducer():
    sock = socket.socket(socket.AF_INET, # Internet
        socket.SOCK_DGRAM) # UDP
    sock.bind(('0.0.0.0', 5002))
    while True:
        data_r, addr_r = sock.recvfrom(1024)
        #Receive join request, send the predecessors information to the new joined VM.
        if data_r.startswith(b'JR'):
            requestor = (int(data_r.decode('utf-8').split(',')[1].split(' ')[0]), data_r.decode('utf-8').split(',')[1].split(' ')[1])
            for i in range(len(Membership)):
                if Membership[i][0] > requestor[0]:
                    sock.sendto(bytes('RJ,'+str(Membership[i-1][0])+' '+Membership[i-1][1]+' '+Membership[i-1][2]+','+str(time.time()), 'utf-8'), (addr_r[0], 5003))
                    break
                if i == len(Membership)-1:
                    sock.sendto(bytes('RJ,'+str(Membership[i][0])+' '+Membership[i][1]+' '+Membership[i][2]+','+str(time.time()), 'utf-8'), (addr_r[0], 5003))



def join_process():
    global PRED_IP
    global SUCC_IP_1
    global SUCC_IP_2
    global SELF_TS
    sock = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock.bind(('0.0.0.0', 5003)) 
    #Send Join Request
    sock.sendto(bytes('JR,'+str(MACHINENUM)+" "+SELF_IP, 'utf-8'),('172.22.156.202',5002))
    data, addr = sock.recvfrom(1024)
    if data.startswith(b'RJ'):
        resp = data.decode('utf-8').split(',')
        #get the PRED_IP
        PRED_IP = resp[1].split(' ')[1]
        #Get the full membership
        sock.sendto(b"GETMEM 5003",(PRED_IP,5004))
        data_r, addr_r = sock.recvfrom(1024)
        for i in data_r.decode('utf-8').split(','):
            proc_id = ((int(i.split(' ')[0]),i.split(' ')[1],i.split(' ')[2]))
            if proc_id not in Membership:
                bisect.insort(Membership,proc_id)
        #From membership get the successors IP.
        SUCC_IP_1 = Membership[(Membership.index((MACHINENUM,SELF_IP,SELF_TS))+1)%len(Membership)][1]
        SUCC_IP_2 = Membership[(Membership.index((MACHINENUM,SELF_IP,SELF_TS))+2)%len(Membership)][1]
        UPDATE_msgs.append('J,'+str(MACHINENUM)+" "+SELF_IP+" "+SELF_TS)
        logging.info("FROM"+" "+"SELF"+":"+ 'J,'+str(MACHINENUM)+" "+SELF_IP+" "+SELF_TS)

def mem_getter():
    sock = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock.bind(('0.0.0.0', 5004))
    #Send the full membership list to the join process.
    while True:
        data, addr = sock.recvfrom(1024)
        #if data:
           #msg, port = data.decode('utf-8').split(' ')
        if data.startswith(b'GETMEM'):
            msg, port = data.decode('utf-8').split(' ')
            mem_Intro = bytes(','.join([str(item[0])+' '+item[1]+' '+item[2] for item in Membership]),"utf-8")
            sock.sendto(mem_Intro,(addr[0],int(port)))

def command_handler():
    global LOSS_RATE
    #Allow user to "list_mem","list_pred"
    sock = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock.bind(('0.0.0.0', 5005))
    while True:
        data, addr = sock.recvfrom(1024)
        if data == b'GETMEM_t':
            mem_Intro = bytes(','.join([str(item[0])+' '+item[1]+' '+item[2] for item in Membership]),"utf-8")
            sock.sendto(mem_Intro,(addr[0],5006))
        if data == b'GETSELF':
            sock.sendto(bytes(str(MACHINENUM)+' '+SELF_IP+' '+SELF_TS, 'utf-8'), (addr[0],5006))
        if data == b'GETPRED':
            sock.sendto(bytes(PRED_IP, 'utf-8'), (addr[0],5006))
        if data == b'GETSUCC1':
            sock.sendto(bytes(SUCC_IP_1, 'utf-8'), (addr[0],5006))
        if data == b'GETSUCC2':
            sock.sendto(bytes(SUCC_IP_2, 'utf-8'), (addr[0],5006))
        if data == b'SETLOSS_3':
            LOSS_RATE = 3
        if data == b'SETLOSS_30':
            LOSS_RATE = 30
        if data == b'SETLOSS_0':
            LOSS_RATE = 0
        if data == b'GETLOSS':
            sock.sendto(bytes(str(LOSS_RATE), 'utf-8'), (addr[0],5006))

        
def check_joined():
    #Avoid duplicate join
    sock = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    res = False
    try:
        sock.bind(('0.0.0.0', 5001))
        res = True
    except:
        print('Already joined!')
    sock.close()
    return res



#If check_joined,then start the thread of server and client
if check_joined():
    if MACHINENUM == 0:
        threading.Thread(target=introducer).start()
    
    threading.Thread(target=mem_getter).start()
    threading.Thread(target=command_handler).start()
        
    join_process()

    threading.Thread(target=server).start()
    threading.Thread(target=client).start()
    
    sock_election = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_election.bind(('127.0.0.1', 5007))
    data, addr = sock_election.recvfrom(1024)
    if data == b'ELEC':
        print('Starting introducer...')
        threading.Thread(target=introducer).start()

