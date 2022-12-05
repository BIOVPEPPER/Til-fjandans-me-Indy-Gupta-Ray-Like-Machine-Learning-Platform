import hashlib
import socket
import sys
import threading
import time
import logging
import re
import os
import numpy as np
from Client import client as callSDFS



SIZE = 4096
FORMAT = 'utf-8'
JOBCNT = 0 # Local counter to generate Job id
JOBS = [None, None] # store tracker objects
SCHEDULER = None # store Scheduler objects
RECENT = ['' for i in range(10)] # record RECENT failure
LATESTJOB1 = '' # record the latest job done by JOBS[0], used for C4
LATESTJOB2 = ''

with open('../config.txt') as f:
    line = f.readlines()[0]
    MACHINENUM, SELF_IP = int(line.split(" ")[0].strip()), line.split(" ")[1].strip()


'''UDP socket objects'''
getmem_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
getmem_UDP.bind(('0.0.0.0',8002))

sock_JOB1_A = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock_JOB1_A.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock_JOB1_A.settimeout(1)
sock_JOB1_A.bind(('0.0.0.0', 8008))
sock_JOB1_F = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock_JOB1_F.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock_JOB1_F.settimeout(1)
sock_JOB1_F.bind(('0.0.0.0', 8010))

sock_JOB2_A = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock_JOB2_A.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock_JOB2_A.settimeout(1)
sock_JOB2_A.bind(('0.0.0.0', 8009))
sock_JOB2_F = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock_JOB2_F.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock_JOB2_F.settimeout(1)
sock_JOB2_F.bind(('0.0.0.0', 8011))

'''Helper for splitting files based on batch size'''
def split_file(filename,batch_size,job_id):
    file = open(filename, 'r')
    lines = file.readlines()
    file.close()
    line_list = []
    for line in lines:
        line_list.append(line)
    count = len(line_list) 
    diff_match_split = [line_list[i:i+batch_size] for i in range(0,len(line_list),batch_size)]
    for j in range(len(diff_match_split)):
         with open(filename+'_'+str(job_id)+'_'+str(j),'w+') as temp:
            for line in diff_match_split[j]:
                temp.write(line)
    
        
    return len(diff_match_split)

'''Helper for counting file lines'''
def buf_count_newlines_gen(fname):
    def _make_gen(reader):
        while True:
            b = reader(2 ** 16)
            if not b: break
            yield b

    with open(fname, "rb") as f:
        count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
    return count

'''a trakcer object is created whenever a job is submitted, responsible for tracking status of each job'''
class tracker:
    def __init__(self, filename:str, jobtype:str, ID:int, 
                    batch_size:int, members:dict, sockA:socket, 
                    sockF:socket, clientIP:str, lineCnt:int) -> None:
        '''
            filename: str, SDFS file name to be inferenced
            jobtype: str, languange or vision
            ID: int, Job ID
            batch_size: int, batch_size
            members: dict, entries of mem_list
            machinedict: dict, status of all members, None(available) or queryName(occupied)
            done: list, queries processed and the processor
            todo: list, query to be processed
            numQuery: int, total number of queries
            sockA: socket, a UDP socket object to receive ack message
            sockF: socket, a UDP socket object to receive finish message
            status: int, -1 created, 0 initialized, 1 training, 2 trained, 3 inferencing, 4 inferenced
            clientIP: str, IP address of the client that submit this job
            lineCnt: int, line count of the file to be inferenced
        '''
        self.data = filename
        self.ID = ID
        self.type = jobtype
        self.batch_size = batch_size
        self.machinedict = {}
        self.members = members
        self.done = []
        self.todo = []
        self.numQuery = 0
        self.sockA = sockA
        self.sockF = sockF
        self.status = -1
        self.clientIP = clientIP
        self.lineCnt = lineCnt
        self.toRemove = 0#New
        self._stopEvent = threading.Event()
        

    '''Split file based on batch size and store in SDFS'''
    def prepare_files(self):
        self.numQuery = split_file(self.data,self.batch_size,self.ID)
        for i in range(self.numQuery):
            #Task name distributed for each machine
            task_filename = self.data+'_'+str(self.ID)+'_'+str(i)
            #Store files into SDFS
            while True:
                exitCode = callSDFS('WR', task_filename, task_filename)
                if exitCode == 1:
                    break
                else:
                    time.sleep(0.5)
            # Append file into the todo list.
            self.todo.append(task_filename)
        self.todo = self.todo + self.todo + self.todo + self.todo + self.todo

    
    '''Monitor and distribute tasks in inference phase'''
    def inference(self):
        self.status = 3
        #Initial assignment
        self.machinedict = {vm:None for vm in self.members.keys()}
        for vm in list(self.machinedict.keys()):
            task = self.todo.pop(0)
            self.sockA.sendto(('IT '+ task).encode(FORMAT), (self.members[vm][0], 8001))
            ack, _ = self.sockA.recvfrom(SIZE)
            if ack.startswith(b'IACK'):
                self.machinedict[vm] = task
        # Whenever a machine finish a task, assign a new task to it
        while True:
            if self._stopEvent.isSet():
                print('Job terminating!')
                break
            try:
                msg, addr = self.sockF.recvfrom(SIZE)
            except:
                continue
            if msg.startswith(b'TF'):
                taskDone = msg.decode(FORMAT).split(' ')[1][:-4]
                self.done.append((taskDone, str(time.time()), len(self.members)))
                # If all job finished, start aggregation procedure
                if len(self.todo) == 0:
                    if len(self.done) == self.numQuery:
                        self.status = 4
                        break
                    else:
                        for vm in list(self.machinedict.keys()):
                            if vm not in self.machinedict:
                                continue
                            if self.machinedict[vm] == taskDone:
                                self.machinedict[vm] = None
                        continue
                # If remove_member is called, the next member that finish a task will be removed
                if self.toRemove == 1:
                    for vm in list(self.machinedict.keys()):
                        if vm not in self.machinedict:
                            continue
                        if self.machinedict[vm] == taskDone:
                            self.toRemove = (vm, self.members[vm])
                    continue
                # Assign a new task to the VM that just finished a task
                for vm in list(self.machinedict.keys()):
                    if vm not in self.machinedict:
                        continue
                    if self.machinedict[vm] == taskDone:
                        task = self.todo.pop(0)
                        self.sockA.sendto(('IT '+ task).encode(FORMAT), (self.members[vm][0], 8001))
                        try:
                            ack, _ = self.sockA.recvfrom(SIZE)
                        except:
                            ack = b'NA'
                        if ack.startswith(b'IACK'):
                            self.machinedict[vm] = task
                        # If did not receive ack, check if the worker is alive
                        elif ack.startswith(b'NA'):
                            self.sockA.sendto(('LT').encode(FORMAT), (self.members[vm][0], 8001))
                            try:
                                ack, _ = self.sockA.recvfrom(SIZE)
                            except:
                                ack = b'NA'
                            if ack.startswith(b'LTACK'):
                                if ack.decode(FORMAT).split(' ')[1] == task:
                                        self.machinedict[vm] = task
                            else:
                                self.machinedict[vm] = None
                                self.todo.append(task)
                    elif self.machinedict[vm] == None:
                        try:
                            task = self.todo.pop(0)
                        except:
                            continue
                        self.sockA.sendto(('IT '+ task).encode(FORMAT), (self.members[vm][0], 8001))
                        try:
                            ack, _ = self.sockA.recvfrom(SIZE)
                        except:
                            ack = b'NA'
                        if ack.startswith(b'IACK'):
                            self.machinedict[vm] = task
                        # If did not receive ack, check if the worker is alive
                        elif ack.startswith(b'NA'):
                            self.sockA.sendto(('LT').encode(FORMAT), (self.members[vm][0], 8001))
                            try:
                                ack, _ = self.sockA.recvfrom(SIZE)
                            except:
                                ack = b'NA'
                            if ack.startswith(b'LTACK'):
                                if ack.decode(FORMAT).split(' ')[1] == task:
                                        self.machinedict[vm] = task
                            else:
                                self.machinedict[vm] = None
                                self.todo.append(task)

                #print(f'Job {self.ID} Done: {len(self.done)*self.batch_size} ToDo: {len(self.todo)*self.batch_size} InProgress: {sum([1 for val in self.machinedict.values() if val not in [None, "Dead"]])*self.batch_size}')

    '''Collect results and aggregate into one file'''
    def collect_res(self):
        print(f'Job {self.ID} done, collecting results...')
        for shard in self.done:
            while True:
                exitCode = callSDFS('RR', shard[0]+'_inf', shard[0]+'_inf')
                if exitCode == 1:
                    break
                else:
                    time.sleep(0.5)
        with open('Job'+str(self.ID)+'_translated','w+') as temp:
            for shard in self.done:
                data_got = open(shard[0]+'_inf', encoding = 'utf-8')
                data_got = data_got.read()
                temp.write(data_got)
        return 'Job'+str(self.ID)+'_translated'

    '''Add a new member into this jobs'''
    def add_member(self, new_machine):
        new_machine_ID,new_IP = new_machine[0],new_machine[1][0]#new
        # Ask the worker to change to this job
        self.sockA.sendto(('CJ '+str(self.ID)+' '+self.type).encode(FORMAT),(new_IP,8001))
        try:
            ack, _ = self.sockA.recvfrom(SIZE)
        except:
            ack = b'NA'
        # If the worker respond, add it into member list and assign task to it.
        if ack.startswith(b'CJACK'):
            self.members[new_machine_ID] = new_machine[1]
            if self.todo == []:
                self.machinedict[new_machine_ID] = None
                return
            task = self.todo.pop(0)
            self.sockA.sendto(('IT '+ task).encode(FORMAT), (self.members[new_machine_ID][0], 8001))
            try:
                ack, _ = self.sockA.recvfrom(SIZE)
            except:
                ack = b'NA'
            if ack.startswith(b'IACK'):
                self.machinedict[new_machine_ID] = task
            elif ack.startswith(b'NA'):
                self.sockA.sendto(('LT').encode(FORMAT), (self.members[new_machine_ID][0], 8001))
                try:
                    ack, _ = self.sockA.recvfrom(SIZE)
                except:
                    ack = b'NA'
                if ack.startswith(b'LTACK'):
                    if ack.decode(FORMAT).split(' ')[1] == task:
                        self.machinedict[new_machine_ID] = task
                else:
                    self.machinedict[new_machine_ID] = None
                    self.todo.append(task)
        else:
            self.members[new_machine_ID] = new_machine[1]
            self.machinedict[new_machine_ID] = None
    
    '''Remove a member, so that it can be assigned to other jobs'''
    def remove_member(self):
        if sum([1 for item in list(self.machinedict.items()) if item[1] != None]) <= 1:
            return 'Denied!'
        self.toRemove = 1
        while True:
            if self.toRemove != 1:
                break
        res = self.toRemove
        self.toRemove = 0
        self.members.pop(res[0])
        self.machinedict.pop(res[0])
        return res

    '''Instruct workers to train models'''
    def train(self):
        self.status = 1
        SJmsg = "SJ"+' '+str(self.ID)+" "+self.type+ " "+str(self.batch_size)
        for vm in self.members.keys():
            print(f'Instructing {self.members[vm][0]} to train...')
            self.sockA.sendto(SJmsg.encode(FORMAT), (self.members[vm][0], 8001))
        ack_count = 0
        while True:
            try:
                ack, _ = self.sockA.recvfrom(SIZE)
            except:
                continue
            if ack.startswith(b'SJACK'):
                ack_count += 1
            if ack_count == len(self.members):
                break
        print(f'Job {self.ID} Train finished!')
        self.status = 2
    
    '''Monitor recent failure message, remove failed member from member list.'''
    def RECENT_monitor(self):
        last_fail = ''
        while True:
            if RECENT[-1] == '':
                continue
            for member in list(self.members.items()):
                if member[0] + ' ' + member[1][0] + ' ' + member[1][1] in RECENT:
                    print('Failed member found!')
                    print(member[0] + ' ' + member[1][0] + ' ' + member[1][1])
                    self.members.pop(member[0])
                    if self.machinedict[member[0]] not in [None, 'Dead']:
                        task = self.machinedict.pop(member[0])
                        self.todo.append(task)
                    else:
                        self.machinedict.pop(member[0])

            last_fail = RECENT[-1]

    '''Set stop event, collect inference result and respond to client'''
    def stop(self):
        print(f'Stopping Job {self.ID}...')
        self._stopEvent.set()
        res = self.collect_res()
        self.sockF.sendto(('F '+str(self.ID)+' '+res).encode(FORMAT), (self.clientIP, 8005))
    
    '''Calculate query rate'''
    def query_rate(self, t):
        if self.done == []:
            return -1
        if self.todo == []:
            return -1
        cur_time = time.time()
        cnt = 0
        for task in reversed(self.done):
            if cur_time - float(task[1]) <= t:
                cnt += 1
        return cnt * self.batch_size / t
    
    '''An attempt to estimate query rate more  accurately'''
    def queryrate_est(self):
        if self.done == []:
            return -1
        if self.todo == []:
            return -1
        
        population = []
        donelist = self.done
        curNum = donelist[0][2]
        startTime = donelist[0][1]
        curCnt = self.batch_size
        for i, record in enumerate(donelist):
            if i <= 1:
                continue
            if record[2] == curNum:
                curCnt += self.batch_size
            else:
                population.append(curCnt / (float(donelist[i-1][1]) - float(startTime)) / curNum)
                curNum = record[2]
                startTime = donelist[i-1][1]
                curCnt = self.batch_size
        if donelist[i-1][1] != startTime:
            population.append(curCnt / (float(donelist[i-1][1]) - float(startTime)) / curNum)
        return float(np.mean(population))


    '''Start inference phase'''
    def run_inference(self):
        threading.Thread(target=self.inference).start()
        threading.Thread(target=self.RECENT_monitor).start()

'''A Scheduler object is created when there are two jobs in the system, responsible for resource balancing'''
class Scheduler:
    def __init__(self) -> None:
        self._stopEvent = threading.Event()
    
    '''Initial assignment of resources'''
    def calc_resource(self, type1, type2, num_machines):
        prior = {'V': 1, 'T':1}
        num_machine1 = round(prior[type1]/(prior[type1]+prior[type2])*num_machines)
        num_machine2 = num_machines - num_machine1
        return num_machine1, num_machine2
    
    '''Start monitoring inference rate of jobs, reassign resources when necessary'''
    def resource_monitor(self):
        time.sleep(5)
        while True:
            if self._stopEvent.isSet():
                print('Job terminating!')
                break
            if None in JOBS:
                break
            # Check if jobs has ended
            if JOBS[0].status == 4 and JOBS[1].status == 4:
                print('Both Jobs ended!')
            # If one job ends and the other is not, assign all resources to the later
            if JOBS[0].status == 4 or JOBS[1].status == 4:
                print('One of Jobs ended!')
                if JOBS[0].status == 4 and JOBS[1].status <= 4:
                    print('Job 0 ended!')
                    for vm in JOBS[0].members.keys():
                        mem = (vm, JOBS[0].members[vm])
                        if mem == 'Denied!':
                            break
                        JOBS[1].add_member(mem)
                        print('Machine '+mem[0]+' added to Job '+str(JOBS[1].ID))
                    JOBS[0].members = {}
                else:
                    print('Job 1 ended!')
                    for vm in JOBS[1].members.keys():
                        mem = (vm, JOBS[1].members[vm])
                        if mem == 'Denied!':
                            break
                        JOBS[0].add_member(mem)
                        print('Machine '+mem[0]+' added to Job '+str(JOBS[0].ID))
                    JOBS[1].members = {}
                break

            # Calculate currnet query rate
            qqrate1 = JOBS[0].query_rate(10)
            qqrate2 = JOBS[1].query_rate(10)
            if qqrate1 == -1 or qqrate2 == -1:
                continue
            qqrate1 = max(qqrate1, 0.01)
            qqrate2 = max(qqrate2, 0.01)

            # Check if resource assignment need to be adjusted
            if 1.2 < qqrate1 / qqrate2 or qqrate1 / qqrate2 < 0.83:
                machine_num1 = len(JOBS[0].members)
                machine_num2 = len(JOBS[1].members)
                getmem_UDP.sendto(b'GETMEM 8002', ('127.0.0.1', 5004))
                #get mem_list from membership
                mem_list, _ = getmem_UDP.recvfrom(1024)
                mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
                if '0' in mem_list:
                    total_machine = len(mem_list) - 2
                else:
                    total_machine = len(mem_list) - 1
                # Only reassign one VM at a time
                if qqrate1 < qqrate2:
                    mem = JOBS[1].remove_member()
                    if mem == 'Denied!':
                        continue
                    #print('Machine '+mem[0]+' removed from Job '+str(JOBS[1].ID))
                    JOBS[0].add_member(mem)
                    #print('Machine '+mem[0]+' added to Job '+str(JOBS[0].ID))
                    time.sleep(3)
                else:
                    mem = JOBS[0].remove_member()
                    if mem == 'Denied!':
                        continue
                    #print('Machine '+mem[0]+' removed from Job '+str(JOBS[0].ID))
                    JOBS[1].add_member(mem)
                    #print('Machine '+mem[0]+' added to Job '+str(JOBS[1].ID))
                    time.sleep(3)

    '''Set stop event'''
    def stop(self):
        print(f'Stopping Scheduler...')
        self._stopEvent.set()
    
    '''Start monitoring'''
    def run(self):
        threading.Thread(target=self.resource_monitor).start() 
        

'''Route ack messages to corresponding job tracker'''
def ackRouter():
    global JOBS
    sockAck = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sockAck.bind(('0.0.0.0',8012))

    while True:
        Ack, addr = sockAck.recvfrom(SIZE)
        JobID = int(Ack.decode(FORMAT).split(' ')[-1])
        if JOBS[0] != None:
            if JOBS[0].ID == JobID:
                sockAck.sendto(Ack, ('0.0.0.0', 8008))
        if JOBS[1] != None:
            if JOBS[1].ID == JobID:
                sockAck.sendto(Ack, ('0.0.0.0', 8009))




'''Route other messages to corresponding job tracker'''
def messageRouter():
    global JOBS
    global LATESTJOB1
    global LATESTJOB2
    sockFini = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sockFini.bind(('0.0.0.0',8006))

    while True:
        msg, addr = sockFini.recvfrom(SIZE)
        if msg.startswith(b'TF'):
            JobID = int(msg.decode(FORMAT).split(' ')[-1])
            if JOBS[0] != None:
                if JOBS[0].ID == JobID:
                    sockFini.sendto(msg, ('0.0.0.0', 8010))
                    LATESTJOB1 = msg.decode(FORMAT).split(' ')[1] + ' ' + addr[0]
            if JOBS[1] != None:
                if JOBS[1].ID == JobID:
                    sockFini.sendto(msg, ('0.0.0.0', 8011))
                    LATESTJOB2 = msg.decode(FORMAT).split(' ')[1] + ' ' + addr[0]


'''Record failure messages, modify META accordingly, and determine files that need to be replicated'''
def failureListener():
    global RECENT
    sock_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_UDP.bind(('0.0.0.0', 8015))

    while True:
        msg, _ = sock_UDP.recvfrom(1024)
        # Only proceed if get fail warning
        if msg.startswith(b'F'):
            failed = msg.decode(FORMAT)[3:-1].split(', ')
            failed = failed[0] + ' ' + failed[1][1:-1] + ' ' + failed[2][1:-1]
            # If new failed process, delete process in all META entries, and 
            # record the files on that process in ToBeRep
            if failed not in RECENT:
                RECENT.pop(0)
                RECENT.append(failed)

'''Handle commands C1 to C5'''
def command_handler():
    sock_COM = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_COM.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock_COM.bind(('0.0.0.0', 8016))
    while True:
        request, addr = sock_COM.recvfrom(SIZE)
        if request.startswith(b'C1'):
            res = {}
            for job in JOBS:
                if job != None:
                    qrate = job.query_rate(20)
                    done = len(job.done) * job.batch_size
                    res[job.ID] = (qrate, done)
            sock_COM.sendto(str(res).encode(FORMAT), (addr[0], 5006))
        elif request.startswith(b'C2'):
            res = {}
            for job in JOBS:
                if job != None:
                    jid = job.ID
                    res[jid] = []
                    done = job.done
                    bsize = job.batch_size
                    population = []
                    if len(done) <=1:
                        res[job.ID] = 'Not enough data.'
                        continue
                    for i in range(1, len(done)):
                        time = float(done[i][1]) - float(done[i-1][1])
                        for j in range(bsize):
                            population.append(bsize / time)
                    res[jid].append(float(np.mean(population)))
                    res[jid].append(float(np.percentile(population, 25)))
                    res[jid].append(float(np.percentile(population, 50)))
                    res[jid].append(float(np.percentile(population, 75)))
                    res[jid].append(float(np.std(population)))
            sock_COM.sendto(str(res).encode(FORMAT), (addr[0], 5006)) 
        elif request.startswith(b'C3'):
            _, jid, bsize = request.decode(FORMAT).split(' ')
            for job in JOBS:
                if job.ID == jid:
                    job.batch_size = bsize
        elif request.startswith(b'C4'):
            jid = request.decode(FORMAT).split(' ')[1]
            if int(jid) == JOBS[0].ID:
                latestjob = LATESTJOB1
            elif  int(jid) == JOBS[1].ID:
                latestjob = LATESTJOB2
            filename, target = latestjob.split(' ')
            sock_COM.sendto(('C4 '+filename).encode(FORMAT), (target, 8017))
            res, _ = sock_COM.recvfrom(4096)
            sock_COM.sendto(res, (addr[0], 5006))
        elif request.startswith(b'C5'):
            res = {}
            for job in JOBS:
                if job != None:
                    res[job.ID] = list(job.members.keys())
            sock_COM.sendto(str(res).encode(FORMAT), (addr[0], 5006))


'''Main server is responsible to listen to requests from clients and create objects when necessary.'''
def main_SERVER():
    global JOBS
    global JOBCNT
    global SCHEDULER
    requestRecv_UDP = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    requestRecv_UDP.bind(('0.0.0.0',8004))
    while True:
        request, addr = requestRecv_UDP.recvfrom(1024)
        request = request.decode('utf-8')
        print("Received Request"+" "+request)
        # If request is job submission, create tracker
        if request.startswith('SUB'):
            if None not in JOBS:
                requestRecv_UDP.sendto(b'SUBNACK -1', (addr[0], 8005))
                continue
            getmem_UDP.sendto(b'GETMEM 8002', ('127.0.0.1', 5004))
            #get mem_list from membership
            mem_list, _ = getmem_UDP.recvfrom(1024)
            mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
            mem_list.pop(str(MACHINENUM),None)
            mem_list.pop(str(MACHINENUM+1),None)
            jtype, filename, batch_size = request.split(' ')[1:]
            while True:
                exitCode = callSDFS('RR', filename, filename)
                if exitCode == 1:
                    break
                else:
                    time.sleep(0.5)
            line_cnt = buf_count_newlines_gen(filename)
            if JOBS[0] == None:
                JOBS[0] = tracker(filename, jtype, JOBCNT, int(batch_size),
                                    mem_list, sock_JOB1_A, sock_JOB1_F, addr[0], line_cnt)
            elif JOBS[1] == None:
                # If more than one job submitted, create scheduler object
                scheduler = Scheduler()
                num_machine1, num_machine2 = scheduler.calc_resource(JOBS[0].type, jtype, len(mem_list))
                JOBS[0].members = {key:mem_list[key] for i, key in enumerate(mem_list.keys()) if i < num_machine1}
                member = {key:mem_list[key] for i, key in enumerate(mem_list.keys()) if i >= num_machine1}
                JOBS[1] = tracker(filename, jtype, JOBCNT, int(batch_size),
                                    member, sock_JOB2_A, sock_JOB2_F, addr[0], line_cnt)
                SCHEDULER = scheduler
            JOBCNT += 1
            requestRecv_UDP.sendto(('SUBACK '+str(JOBCNT-1)).encode(FORMAT), (addr[0], 8005))
        # If request is start training
        if request.startswith('STR'):
            for J in JOBS:
                if J != None:
                    J.prepare_files()
                    J.train()
                    requestRecv_UDP.sendto(('STRACK '+str(J.ID)).encode(FORMAT), (addr[0], 8005))
        # If request is start inference
        if request.startswith('SINF'):
            for J in JOBS:
                if J != None:
                    J.run_inference()
                    requestRecv_UDP.sendto(('SINFACK '+str(J.ID)).encode(FORMAT), (addr[0], 8005))
            if SCHEDULER != None:
                SCHEDULER.run()
        # If request is end jobs
        if request.startswith('EJ'):
            getmem_UDP.sendto(b'GETMEM 8002', ('127.0.0.1', 5004))
            #get mem_list from membership
            mem_list, _ = getmem_UDP.recvfrom(1024)
            mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
            mem_list.pop(str(MACHINENUM),None)
            mem_list.pop(str(MACHINENUM+1),None)
            for vm in mem_list.keys():
                requestRecv_UDP.sendto(b'EJ', (mem_list[vm][0], 8001))
                ack, _ = requestRecv_UDP.recvfrom(SIZE)
            for i, J in enumerate(JOBS):
                if J != None:
                    J.stop()
                    requestRecv_UDP.sendto(('EJACK '+str(J.ID)).encode(FORMAT), (addr[0], 8005))
                    JOBS[i] = None
            if SCHEDULER != None:
                SCHEDULER.stop()
                SCHEDULER = None



threading.Thread(target=main_SERVER).start() 
threading.Thread(target=ackRouter).start()
threading.Thread(target=messageRouter).start()    
threading.Thread(target=command_handler).start() 
threading.Thread(target=failureListener).start()
