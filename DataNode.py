import socket
import re
import queue
import threading
import os
import shutil
import logging
import time


MSGS_buff = queue.PriorityQueue()
MSGS = queue.PriorityQueue()
FILE_LIST = dict()
META_backup = tuple()
FORMAT = 'utf-8'
CUR_MASTER = 0
DIR = 'SDFS/'
SIZE = 4096
logging.basicConfig(filename="SDFSDNode.log", level=logging.INFO)


'''Clear all files in SDFS directory'''
if os.path.exists(DIR):
    for filename in os.listdir(DIR):
        file_path = os.path.join(DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))
else:
    os.mkdir(DIR)


'''Function used to transfer a file to another data node'''
def transfer_client(msg,ip):
    ADDR = (ip, 6003)
    SIZE = 1024
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    """ Connecting to target node. """
    while True:
        try:
            client.connect(ADDR)
            break
        except:
            continue

    fileName, verNum, replicas = re.split('(W) (.*) (\d+\.*\d*) (\[.*\])', msg)[2:5]


    """ Opening and reading the file data. """
    with open(DIR+fileName+'_v'+verNum, 'rb') as file:
        data = file.read()
    """ Sending the filename to the target node. """
    client.send(msg.encode(FORMAT))
    msg = client.recv(SIZE).decode(FORMAT)
    """ Sending the file data to the target node. """
    client.sendall(data)
 
    """ Closing the file. """
    file.close()
 
    """ Closing the connection from the server. """
    client.close()
    logging.info(str(time.time()) + " S"+" "+fileName+" v"+ str(verNum) + ' '+ ip)




      


'''Function for listening and handling write request from client.'''
def WriteConsumer():
    global FILE_LIST
    global SIZE
    IP = '0.0.0.0'
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #SIZE = 1024
    PORT = 6003
    ADDR = (IP,PORT)
    server.bind(ADDR) 
    server.listen(10)
    print("[LISTENING] WriteConsumer is listening.")

    """A UDP socket to interact with membership service"""
    sock_UDP = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock_UDP.bind(('0.0.0.0', 6000))
 
    while True:
        """ Acept the connection from the client. """
        conn, addr = server.accept()
 
        """ Receiving the filename from the client. """
        msg = conn.recv(SIZE).decode(FORMAT)
        fileName, verNum, replicas = re.split('(W) (.*) (\d+\.*\d*) (\[.*\])', msg)[2:5]
        conn.send("Write request received.".encode(FORMAT))

        """ Receiving the file data from the client. """
        with open(DIR + fileName+'_v'+verNum, 'wb') as file:
            while True:
                data = conn.recv(SIZE)
                if not data:
                    break
                file.write(data)
        """ Closing the connection from the client. """
        conn.close()
        # Update current stored version number of this file
        if int(verNum) == 1:
            FILE_LIST[fileName] = [1]
        else:
            FILE_LIST[fileName].append(int(verNum))

        """Send Ack to coordinator"""
        sock_UDP.sendto(b'GETMAS 6000', ('127.0.0.1', 6019))
        masNum, _ = sock_UDP.recvfrom(1024)
        sock_UDP.sendto(b'GETMEM 6000', ('127.0.0.1', 5004))
        mem_list, _ = sock_UDP.recvfrom(1024)
        mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}

        sock_UDP.sendto(('WACK '+ fileName+ ' ' + verNum).encode(FORMAT), (mem_list[masNum.decode(FORMAT)][0], 6002))
        logging.info(str(time.time()) + " W"+" "+fileName+" v"+ str(verNum))

        
        """Flush to the next replica, need to interact with membership service"""
        replicas = replicas[1:-1].split(', ')
        if len(replicas) > 1:
            sock_UDP.sendto(b'GETMEM 6000', ('127.0.0.1', 5004))
            mem_list, _ = sock_UDP.recvfrom(1024)
            mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
            transfer_client('W '+ fileName + ' ' + verNum + ' ' + str([int(i) for i in replicas[1:]]), 
                            mem_list[replicas[1]][0]) 

        
'''Function for listening and handling read request from client.'''
def ReadConsumer():
    IP = '0.0.0.0'
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    SIZE = 1024
    PORT = 6007
    ADDR = (IP,PORT)
    server.bind(ADDR) 
    server.listen()
    print("[LISTENING] ReadConsumer is listening.")
    while True:
        """Accepted the connection from the client. """
        conn, addr = server.accept()

        msg = conn.recv(SIZE).decode(FORMAT)

        '''Send a specific version of file to requestor.'''
        if msg.startswith('R'):
            fileName, verNum = re.split('R (.*) ([0-9L]*)', msg)[1:-1]

            if verNum == 'L':
                verNum = FILE_LIST[fileName][-1]

            with open(DIR + fileName+'_v'+str(verNum), "rb") as file:
                data = file.read()
            conn.sendall(data)

            file.close()

            conn.close()


'''Helper for sending the version numbers of a file'''
def verNumGetter():
    global FILE_LIST
    sock_UDP = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock_UDP.bind(('0.0.0.0', 6006))

    while True:
        data, addr = sock_UDP.recvfrom(1024)
        if data.startswith(b'RV'):
            fileName = data.decode('utf-8').split(' ')[1]
            if fileName in FILE_LIST:
                sock_UDP.sendto(('RVR '+fileName+' '+str(FILE_LIST[fileName])).encode(FORMAT), (addr[0], 6005))
            else:
                sock_UDP.sendto(('RVR '+fileName+' [0]').encode(FORMAT), (addr[0], 6005))

'''Function for listening and handling delete request from coordinator.'''
def DeleteConsumer():
    global FILE_LIST
    sock_UDP = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock_UDP.bind(('0.0.0.0', 6008))
    print("[LISTENING] DeleteConsumer is listening.")

    while True:
        data, addr = sock_UDP.recvfrom(1024)
        if data.startswith(b'D'):
            fileName = data.decode('utf-8').split(' ')[1]
            # If file exists, delte.
            if fileName in FILE_LIST:
                for verNum in FILE_LIST[fileName]:
                    os.remove(DIR + fileName+'_v'+str(verNum))
                del FILE_LIST[fileName]
            sock_UDP.sendto(('DACK '+ fileName).encode(FORMAT), (addr[0], 6010))
            logging.info(str(time.time()) + " D"+" "+fileName)


'''Function for listening and handling re-replication instruction from coordinator.'''
def repInstConsumer():
    
    sock_UDP = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock_UDP.bind(('0.0.0.0', 6009))

    while True:
        msg, addr = sock_UDP.recvfrom(1024)
        if msg.startswith(b'REP'):
            # Get file name and target node from coordinator
            filename, target = re.split('(REP) (.*) \[(.*)\]', msg.decode(FORMAT))[2:4]
            sock_UDP.sendto('REP RECEIVED!'.encode(FORMAT), (addr[0], 6018))
            # Transfer all versions of that file to target
            for ver in FILE_LIST[filename]:
                transfer_client('W '+ filename + ' ' + str(ver) + ' ' + str([]), target.split(' ')[1])

'''Function for receiving meta data backup from coordinator'''
def METAreceiver(): 
    global META_backup
    sock_UDP = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock_UDP.bind(('0.0.0.0', 6016))

    while True:
        data, addr = sock_UDP.recvfrom(1024)
        if data.startswith(b'META'):
            META_backup = tuple(re.split('META (\{.*\}) (.*)', data.decode(FORMAT))[1:-1])


'''Function for handling request for META backup'''
def METABackupGetter():
    global META_backup
    sock_UDP = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock_UDP.bind(('0.0.0.0', 6025))

    while True:
        data, addr = sock_UDP.recvfrom(1024)
        if data.startswith(b'GETMETA'):
            if len(META_backup) > 0:
                sock_UDP.sendto((str(META_backup)).encode(FORMAT), (addr[0], 6026))
            else:
                sock_UDP.sendto((str((None, '-1'))).encode(FORMAT), (addr[0], 6026))


'''Function for listing all files stored on current node.'''
def localStoreGetter():
    sock_UDP = socket.socket(socket.AF_INET, # Internet
                        socket.SOCK_DGRAM) # UDP
    sock_UDP.bind(('0.0.0.0', 6021))

    while True:
        data, addr = sock_UDP.recvfrom(1024)
        if data.startswith(b'GETLOCAL'):
            sock_UDP.sendto((str(list(FILE_LIST.keys()))).encode(FORMAT), ('127.0.0.7', 5006))
            
'''Function for replying to coordinator's heat beat check'''
def heartBeat():
    sock_UDP = socket.socket(socket.AF_INET, # Internet
                                    socket.SOCK_DGRAM) # UDP
    sock_UDP.bind(('0.0.0.0', 6023))

    while True:
        data, addr = sock_UDP.recvfrom(1024)
        if data.startswith(b'HB'):
            sock_UDP.sendto(b'Live!', (addr[0], 6024))

            

threading.Thread(target=ReadConsumer).start()
threading.Thread(target=WriteConsumer).start()
threading.Thread(target=verNumGetter).start()
threading.Thread(target=DeleteConsumer).start()
threading.Thread(target=repInstConsumer).start()
threading.Thread(target=METAreceiver).start()
threading.Thread(target=localStoreGetter).start()
threading.Thread(target=heartBeat).start()
threading.Thread(target=METABackupGetter).start()
