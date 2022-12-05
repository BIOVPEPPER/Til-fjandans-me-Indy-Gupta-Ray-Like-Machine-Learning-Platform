import hashlib
import socket
import sys
import threading
import time
import logging
import re

META = {}
FORMAT = "utf-8"
ToBeRep = []
recent = ['' for i in range(10)]
getmem_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
getmem_UDP.bind(('0.0.0.0',6004))
getmem_UDP.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
getVersion_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
getVersion_UDP.bind(('0.0.0.0',6005))
getVersion_UDP.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
getVersion_UDP.settimeout(2)
getHeartBeat = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
getHeartBeat.bind(('0.0.0.0',6024))
getHeartBeat.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
getHeartBeat.settimeout(2)
filenameDict = {}
logging.basicConfig(filename="SDFSCoord.log", level=logging.INFO)

# Hash function used to assign data nodes to a file
def hashing(s):
    hashnum = hashlib.sha1(s.encode('utf-8')).hexdigest()
    int_hash = int(hashnum, 16)%10
    return int_hash

''' Main coordinator function to handle requests from client'''
def server_Coordinator():
    global META
    global filenameDict
    """ Staring a TCP socket. """
    IP = '0.0.0.0'
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    #Staring a UDP package, just for the ACK.
    server_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    
    SIZE = 1024
    PORT = 6001
    PORT_ACK = 6002
    ADDR = (IP,PORT)
    server.bind(ADDR) 
    server_UDP.bind((IP, PORT_ACK))
    
    """ Server is listening, i.e., server is now waiting for the client to connected. """
    server.listen(10)
    print("Coordinator On.")
    while True:
        """ Server has accepted the connection from the client. """
        conn, addr_conn = server.accept()
        server.close()
        """ Receiving the filename from the client. """
        request_name = conn.recv(SIZE).decode(FORMAT)
        request = request_name.split(' ')[0]
        filename = request_name.split(' ')[1]
        
        #Handling write request
        if request == 'WR':
            flag = 0
            # Check if file has existed in SDFS
            if filename in filenameDict.keys():
                filenameDict[filename] += 1
            else:
                filenameDict[filename] =  1
            # Get membership list from membership service
            getmem_UDP.sendto(b'GETMEM 6004', ('127.0.0.1', 5004))
            mem_list, _ = getmem_UDP.recvfrom(1024)
            mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
            # If new file, use hashing to assign nodes to this file
            if filename not in META:
                hashlist = [hashing(filename)%len(mem_list)]
                hashlist.append((hashlist[0]+1)%len(mem_list))
                hashlist.append((hashlist[0]+2)%len(mem_list))
                hashlist.append((hashlist[0]+3)%len(mem_list))
                hashlist_WR = []
                for i, item in enumerate(mem_list.items()):
                    if i in hashlist:
                        hashlist_WR.append(int(item[0]))
                flag=0
            # If not new file, fetch nodes from meta data
            else:
                hashlist_WR = [int(item.split(' ')[0]) for item in META[filename]]
                flag=1
            # Send back list of nodes to client
            respond = "RWR" + " " + filename + " " + str(filenameDict[filename]) + " " + str(hashlist_WR)
            conn.send(respond.encode(FORMAT))
            # Count acks. Reply to client when receive 3 acks.
            count_ACK = 0 
            while True:
                data, addr = server_UDP.recvfrom(1024)
                if data.startswith(b'WACK') and data.decode(FORMAT).split(' ')[1] == filename and data.decode(FORMAT).split(' ')[2] == str(filenameDict[filename]):
                    count_ACK += 1 
                    if count_ACK == 2:
                        #If new file, update meta data
                        if flag == 0:
                            META[filename] = [str(i)+' '+mem_list[str(i)][0]+' '+mem_list[str(i)][1] for i in hashlist_WR]
                        #Back up meta data to data nodes
                        for item in hashlist_WR:
                            server_UDP.sendto(('META '+ str(META) + ' ' + str(time.time())).encode(FORMAT), (mem_list[str(item)][0], 6016))
                        # Reply to client
                        conn.send('Write Finished!'.encode(FORMAT))
                        logging.info(str(time.time()) + " W"+" "+filename+" v"+ str(filenameDict[filename]))
                        break
        elif request == 'RR':
            #If requested file exists
            if filename in META:
                # Fetch list of nodes from meta data
                hashlist_RR = [int(item.split(' ')[0]) for item in META[filename]]
                # Get membership list
                getmem_UDP.sendto(b'GETMEM 6004', ('127.0.0.1', 5004))
                mem_list, _ = getmem_UDP.recvfrom(1024)
                mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
                version_dict = {}
                rep_count = 0
                # Ask data nodes for latest version number. Stop when two of them reply.
                for i in hashlist_RR:
                    if (str(i) + ' ' + mem_list[str(i)][0] + ' ' + mem_list[str(i)][1]) in recent:
                        continue
                    IP = mem_list[str(i)][0]
                    getVersion_UDP.sendto(('RV'+' '+filename).encode(FORMAT),(IP,6006))
                    try:
                        version, _ = getVersion_UDP.recvfrom(1024)
                    except:
                        continue
                    version = re.split('.*(\[[^\[\]]+\])', version.decode(FORMAT))[1]
                    version_dict[IP] = max(eval(version))
                    rep_count += 1
                    if rep_count >= 2:
                        break
                if max(version_dict.values()) == 0:
                    conn.send('FILE NOT FOUND'.encode(FORMAT))
                # Find the node with largest version number
                max_version_ip = max(version_dict,key = version_dict.get)
                # Send the target data node back to client
                conn.send(("RRR"+" "+max_version_ip).encode(FORMAT))
            # If requested file does not exist
            else:
                conn.send('FILE NOT FOUND'.encode(FORMAT))

        elif request == 'DR':
            #If requested file exists
            if filename in META:
                # Fetch list of nodes from meta data
                hashlist_DR = [int(item.split(' ')[0]) for item in META[filename]]
                # Get membership list
                getmem_UDP.sendto(b'GETMEM 6004', ('127.0.0.1', 5004))
                mem_list, _ = getmem_UDP.recvfrom(1024)
                getDELETE_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
                getDELETE_UDP.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                getDELETE_UDP.bind(('0.0.0.0', 6010))
                mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
                count_DACK = 0
                # Send delete message and wait for all 4 acks.
                for i in hashlist_DR:
                    IP = mem_list[str(i)][0]
                    getDELETE_UDP.sendto(('D'+' '+filename).encode(FORMAT),(IP,6008))
                    DACK, _ = getDELETE_UDP.recvfrom(1024)
                    if DACK.startswith(b'DACK'):
                        count_DACK += 1 
                    if count_DACK == 4:
                        backuplist = META[filename]
                        # Update meta data
                        del META[filename]
                        del filenameDict[filename]
                        # Back up meta data to data nodes
                        for item in backuplist:
                            server_UDP.sendto(('META '+ str(META) + ' ' + str(time.time())).encode(FORMAT), (mem_list[item.split(' ')[0]][0], 6016))
                        conn.send('Delete Finished!'.encode(FORMAT))
                        logging.info(str(time.time()) + " D"+" "+filename)
            # If requested file does not exist
            else:
                conn.send('FILE NOT FOUND'.encode(FORMAT))
                    
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        server.bind(ADDR)
        server.listen(10)

            




'''Record failure messages, modify META accordingly, and determine files that need to be replicated'''
def failureListener():
    global META
    global ToBeRep
    global recent
    sock_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_UDP.bind(('0.0.0.0', 6017))

    while True:
        msg, _ = sock_UDP.recvfrom(1024)
        print(msg.decode(FORMAT))
        # Only proceed if get fail warning
        if msg.startswith(b'F'):
            failed = msg.decode(FORMAT)[3:-1].split(', ')
            failed = failed[0] + ' ' + failed[1][1:-1] + ' ' + failed[2][1:-1]
            # If new failed process, delete process in all META entries, and 
            # record the files on that process in ToBeRep
            if failed not in recent:
                recent.pop(0)
                recent.append(failed)
                reps = []
                for file in META.keys():
                    if failed in META[file]:
                        META[file].remove(failed)
                        reps.append(file)
                ToBeRep.append(reps)
                

                
            
'''Send out replicaiton instruction to Data Nodes'''
def repInstructor():
    global META
    global ToBeRep
    global recent
    sock_Inst = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_Inst.bind(('0.0.0.0', 6018))
    sock_Inst.settimeout(1)
    while True:
        # When there are files that need to be replicated, proceed
        if len(ToBeRep) > 0:
            # Make a dict with keys being process name and values being list of files
            getmem_UDP.sendto(b'GETMEM 6004', ('127.0.0.1', 5004))
            mem_list, _ = getmem_UDP.recvfrom(1024)
            mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
            PFmap = {}
            for key in mem_list.keys():
                if (key + ' ' +mem_list[key][0] + ' '+mem_list[key][1]) not in recent: 
                    PFmap[key + ' ' +mem_list[key][0] + ' '+mem_list[key][1]] = set()
            for file in META.keys():
                for p in META[file]:
                    if p in PFmap:
                        PFmap[p].add(file)
            # Get the first entry in ToBeRep
            reps = ToBeRep.pop(0)
            PFmap = sorted([(k, PFmap[k]) for k in PFmap.keys()], key=lambda x: len(x[1]))
            # For every file in reps, find the process that is storing the least files and not storing the target file, 
            # then send replication instruction to that process
            remaining = []
            for f in reps:
                for i, item in enumerate(PFmap):
                    if f not in item[1]:
                        getHeartBeat.sendto(b'HB', (item[0].split(' ')[1], 6023))
                        try:
                            data, _ = getHeartBeat.recvfrom(1024)
                        except:
                            continue
                        temp = (META[f]).copy()
                        for p in temp:
                            if p in recent:
                                continue
                            sock_Inst.sendto(('REP '+f+' ['+item[0]+']').encode(FORMAT), (p.split(' ')[1], 6009))
                            try:
                                response, addr = sock_Inst.recvfrom(1024)
                            except:
                                response, addr = b'Time out!', 'None'
                            if response == b'Time out!':
                                continue
                            else:
                                logging.info(str(time.time()) + " R"+" "+f+' '+p+' '+item[0])
                                break
                        
                        if response == b'Time out!':
                            remaining.append(f)
                        else:
                            PFmap[i][1].add(f)
                            META[f].append(item[0])
                        break
                PFmap = sorted(PFmap, key=lambda x: len(x[1]))
            # If this trial replication fails, re-append this file to ToBeRep
            # so that it can be replicated again later
            if len(remaining) > 1:
                ToBeRep.append(remaining)



'''Send meta data to requester.'''
def METAGetter():
    global META
    sock_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_UDP.bind(('0.0.0.0', 6020))

    while True:
        data, addr = sock_UDP.recvfrom(1024)
        if data.startswith(b'GETMETA'):
            sock_UDP.sendto((str(META)).encode(FORMAT), (addr[0], 5006))

'''Send version dictionary to requester.'''
def versionsGetter():
    sock_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_UDP.bind(('0.0.0.0', 6022))

    while True:
        data, addr = sock_UDP.recvfrom(1024)
        if data.startswith(b'GETVER'):
            version_dict = {}
            filename, num = data.decode(FORMAT).split(' ')[1:]
            num = int(num) 
            storer = META[filename].copy()
            for p in storer:
                if p in recent:
                    continue
                getVersion_UDP.sendto(('RV'+' '+filename).encode(FORMAT),(p.split(' ')[1],6006))
                try:
                    version, _ = getVersion_UDP.recvfrom(1024)
                except:
                    continue
                version = re.split('.*(\[[^\[\]]+\])', version.decode(FORMAT))[1]
                version_dict[p] = eval(version)
            sock_UDP.sendto((str(version_dict)).encode(FORMAT), (addr[0], 5006))


sock_init = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock_init.bind(('0.0.0.0', 6026))
sock_init.settimeout(3)
getmem_UDP.sendto(b'GETMEM 6004', ('127.0.0.1', 5004))
mem_list, _ = getmem_UDP.recvfrom(1024)
mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
max_ts = 0
latest_meta = {}
for val in mem_list.values():
    sock_init.sendto(b'GETMETA', (val[0],6025))
    try:
        data, addr = sock_init.recvfrom(1024)
    except:
        continue
    cur_backup = eval(data.decode(FORMAT))
    if float(cur_backup[1]) > max_ts:
        max_ts = float(cur_backup[1])
        latest_meta = eval(cur_backup[0])
META = latest_meta


threading.Thread(target=server_Coordinator).start()
threading.Thread(target=failureListener).start()
threading.Thread(target=repInstructor).start()
threading.Thread(target=METAGetter).start()
threading.Thread(target=versionsGetter).start()

sock_init.sendto(b'SUCCESS', ('0.0.0.0', 6027))

sock_init.close()

    




