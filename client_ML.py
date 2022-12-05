import socket
import sys
import threading

SIZE = 4096
FORMAT = "utf-8"


'''Send requests to Coordinator'''
def requestConsumer():
    sock_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_UDP.bind(('0.0.0.0',8003))

    while True:
        data, addr = sock_UDP.recvfrom(1024)

        sock_UDP.sendto(b'GETMLMAS 8003', ('127.0.0.1', 6019))
        MLNum, _ = sock_UDP.recvfrom(1024)
        MLNum = int(MLNum)
        sock_UDP.sendto(b'GETMEM 8003', ('127.0.0.1', 5004))
        mem_list, _ = sock_UDP.recvfrom(1024)
        mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}

        if data.startswith(b'SUB'):
            request, jtype, file_name, batch_size=data.decode(FORMAT).split(' ')[0:]
            if MLNum == 0:
                sock_UDP.sendto((request+' '+jtype+' '+file_name+' '+str(batch_size)).encode(FORMAT), 
                                (mem_list[str(MLNum)][0], 8004))
            sock_UDP.sendto((request+' '+jtype+' '+file_name+' '+str(batch_size)).encode(FORMAT), 
                ("172.22.158.202", 8004))
        elif data.startswith(b'STR'):
            if MLNum == 0:
                sock_UDP.sendto(('STR').encode(FORMAT), 
                                (mem_list[str(MLNum)][0], 8004))
            sock_UDP.sendto(('STR').encode(FORMAT), 
                        ("172.22.158.202", 8004))
        elif data.startswith(b'SINF'):
            if MLNum == 0:
                sock_UDP.sendto(('SINF').encode(FORMAT), 
                                (mem_list[str(MLNum)][0], 8004))
            sock_UDP.sendto(('SINF').encode(FORMAT), 
                    ("172.22.158.202", 8004))
        
        elif data.startswith(b'EJ'):
            if MLNum == 0:
                sock_UDP.sendto(('EJ').encode(FORMAT), 
                                (mem_list[str(MLNum)][0], 8004))
            sock_UDP.sendto(('EJ').encode(FORMAT), 
                ("172.22.158.202", 8004))


    

'''Listens message from Coordinator'''
def messageConsumer():
    sock_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_UDP.bind(('0.0.0.0',8005))

    while True:
        data, addr = sock_UDP.recvfrom(1024)

        state, id = data.decode(FORMAT).split(' ')[0], data.decode(FORMAT).split(' ')[1]

        if state == 'SUBACK':
            print('Job submission successful! Job ID: '+id)
        elif state == 'F':
            print('Job submission successful! Result file name: ' + data.decode(FORMAT).split(' ')[-1])
        elif state == 'SUBNACK':
            print('Job submission failed!')
        elif state == 'STRACK':
            print('Job training finished! Job ID: '+id)
        elif state == 'SINFACK':
            print('Job now in inference phase! Job ID: '+id)
        elif state == 'EJACK':
            print('Job terminated. Job ID: '+id)
        else:
            print(f'Job {id} failed! Please try again later!')


threading.Thread(target=requestConsumer).start()
threading.Thread(target=messageConsumer).start()
