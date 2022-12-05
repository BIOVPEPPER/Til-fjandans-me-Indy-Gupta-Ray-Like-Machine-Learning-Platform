import hashlib
import socket
import sys
import threading
import time




def client(request_type,filename,filename_2):
    SIZE = 4096
    FORMAT = "utf-8"
    try:
        client_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        client_UDP.bind(('0.0.0.0',7001))
        client_UDP.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    except:
        return -2


    client_UDP.sendto(b'GETMAS 7001', ('127.0.0.1', 6019))
    masNum, _ = client_UDP.recvfrom(1024)
    client_UDP.sendto(b'GETMEM 7001', ('127.0.0.1', 5004))
    mem_list, _ = client_UDP.recvfrom(1024)
    mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
    
    if masNum.decode(FORMAT) not in mem_list:
        return -3
    IP = mem_list[masNum.decode(FORMAT)][0]
    PORT = 6001
    ADDR = (IP, PORT)
    
    
    # If writing
    if request_type == 'WR':
        client_WR = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_WR.connect(ADDR)
        except:
            client_WR.close()
            return 0
        request_WR = 'WR '+ filename_2
        try:
            client_WR.send(request_WR.encode(FORMAT))
            #Receive store list message from the Coordinator
            msg = client_WR.recv(SIZE).decode(FORMAT)
        except:
            client_WR.close()
            return 0
        timestamp = msg.split(' ')[2]
        # Interpret message to get full lists of nodes
        hashlist = []
        for i in msg.split("[")[-1]:
            try:
                hashlist.append(int(i))
            except:
                pass
        # Get membership list from membership service
        # client_UDP.sendto(b'GETMEM 7001', ('127.0.0.1', 5004))
        # mem_list, _ = client_UDP.recvfrom(1024)
        # mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}

        # The ip address of the node need to be contacted
        IP1 = mem_list[str(hashlist[0])][0]
        port_write = 6003
        ADDR_write = (IP1,port_write)
        client_WN = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_WN.connect(ADDR_write)
        except:
            client_WR.close()
            client_WN.close()
            return -1
        """ Opening and reading the file data. """
        request_W = 'W '+ filename_2+' '+timestamp+" "+str(hashlist)
        try:
            client_WN.send(request_W.encode(FORMAT))
        except:
            client_WR.close()
            client_WN.close()
            return -1
        #file = open(filename, "r")
        #data_file = file.read()
        print(client_WN.recv(SIZE).decode(FORMAT))
        """ Read the file, send all of the file """
        with open(filename, 'rb') as file:
            data_file = file.read()
        try:
            client_WN.sendall(data_file)
        except:
            client_WR.close()
            client_WN.close()
            return -1
        #Close connection with data node
        client_WN.close()
        #Wait for coordinator to reply
        WF = client_WR.recv(SIZE).decode(FORMAT)
        print(WF + ' '+filename)
        client_WR.close()
    # If reading
    elif request_type == 'RR':
        client_RR = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_RR.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        try:
            client_RR.connect(ADDR)
        except:
            client_RR.close()
            return 0
        request_RR = 'RR '+ filename_2
        try:
            client_RR.send(request_RR.encode(FORMAT))
            #Receive store list message from the Coordinator
            msg_RR = client_RR.recv(SIZE).decode(FORMAT)
        except:
            client_RR.close()
            return 0
        # If coordinator says that no such file in SDFS
        if msg_RR == 'FILE NOT FOUND':
            print('FILE NOT FOUND')
            return 1
        # Get the IP address of data node client need to contact
        request_IP = msg_RR.split(' ')[-1]
        port_read = 6007
        ADDR_read = (request_IP,port_read)
        client_RN = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_RN.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        try:
            client_RN.connect(ADDR_read)
        except:
            client_RR.close()
            client_RN.close()
            return -1
        """ Get the file Data """
        request_R = 'R '+ filename_2 +' L'
        try:
            client_RN.send(request_R.encode(FORMAT))
        except:
            client_RR.close()
            client_RN.close()
            return -1
        #-----------------------------------
        #This is for writing a larger file
        with open(filename,'wb') as file:
            while True:
                data = client_RN.recv(SIZE)
                if not data:
                    break
                file.write(data)
        #-----------------------------------
        #Writing Larger File Finished
        print('GET FINISHED')
        #file.close()
        client_RR.close()
        client_RN.close()
    # If deleting
    elif request_type == 'DR':
        client_DR = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_DR.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        try:
            client_DR.connect(ADDR)
        except:
            client_DR.close()
            return 0
        # Send delete request to coordinator
        request_DR = 'DR '+ filename_2
        try:
            client_DR.send(request_DR.encode(FORMAT))
            DF = client_DR.recv(SIZE).decode(FORMAT)
        except:
            client_DR.close()
            return 0
        # Wait for coordinator to reply
        print(DF)
        client_DR.close()
    client_UDP.close()
    return 1


if __name__ == "__main__":
    request=sys.argv[1]
    local_name= sys.argv[2]
    remote_name= sys.argv[3]
    exitCode = client(request,local_name,remote_name)
    if exitCode == 1:
        print('Query Successful!')
    elif exitCode == 0:
        print('Connection to Coordinator failed! Try again later!')
    elif exitCode == -1:
        print('Connection to Data Node failed! Try again later!')
    elif exitCode == -2:
        print('Address already in use! Try again later!')
    elif exitCode == -3:
        print('Recovering from failure! Try again later!')

