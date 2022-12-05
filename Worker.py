import socket
import os
from transformers import MarianMTModel, MarianTokenizer, ViTFeatureExtractor, ViTForImageClassification
import torch
import sys
from collections import deque
import threading
from PIL import Image
import requests
from Client import client as callSDFS
import time

FORMAT = 'utf-8'
with open('../config.txt') as f:
    line = f.readlines()[0]
    MACHINENUM, SELF_IP = int(line.split(" ")[0].strip()), line.split(" ")[1].strip()

'''Receive work instruction from coordinator, and create worker object.'''
def instructionConsumer():
    sock_ML = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_ML.bind(('0.0.0.0',8001))
    sock_ML.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)

    while True:
        data, addr = sock_ML.recvfrom(1024)
        # If instruction if Start job, create worker object and send back ack
        if data.startswith(b'SJ'):
            print(f'Starting Job '+data.decode(FORMAT).split(' ')[1]+'. Job type '+data.decode(FORMAT).split(' ')[2])
            worker = Worker(*(data.decode(FORMAT).split(' ')[1:]))
            if addr[0] == "172.22.156.202":
                sock_ML.sendto(('SJACK '+ data.decode(FORMAT).split(' ')[1]).encode(FORMAT), (addr[0], 8012))
            sock_ML.sendto(('SJACK '+ data.decode(FORMAT).split(' ')[1]).encode(FORMAT), ("172.22.158.202", 8012)) #New, send the SJACK to the StandByCoordinator, to make it update the status.
            worker.run()
            while True:
                train_info, addr = sock_ML.recvfrom(1024)
                # If new job assignment message received from Coordinator
                if train_info.startswith(b'IT'):
                    filename = train_info.decode(FORMAT).split(' ')[1]
                    if addr[0] == "172.22.156.202":
                        sock_ML.sendto(('IACK '+ filename +' ' + str(worker.ID)).encode(FORMAT), ("172.22.156.202", 8012))
                    sock_ML.sendto(('IACK '+ filename +' ' + str(worker.ID)).encode(FORMAT), ("172.22.158.202", 8012))
                    worker.addTask(filename)
                # If end job message received from Coordinator
                elif train_info.startswith(b'EJ'):
                    worker.stop()
                    if addr[0] == "172.22.156.202":
                        sock_ML.sendto(('EJACK '+ filename + ' ' + str(worker.ID)).encode(FORMAT), ("172.22.156.202", 8004))
                    sock_ML.sendto(('EJACK '+ filename + ' ' + str(worker.ID)).encode(FORMAT), ("172.22.158.202", 8012))#New, send it to the StandByCoordinator
                    break
                # If a tracker ask this worker to change job
                elif train_info.startswith(b'CJ'):#new
                    nid, ntype = train_info.decode(FORMAT).split(' ')[1:]
                    worker.changeJob(int(nid), ntype)#new
                    if addr[0] == "172.22.156.202":
                        sock_ML.sendto(('CJACK ' + nid).encode(FORMAT), ("172.22.156.202", 8012))
                    sock_ML.sendto(('CJACK ' + nid).encode(FORMAT), ("172.22.158.202", 8012))
                    print('Job changed to '+nid+', type '+ntype)
                # If a tracker ask to see if this worker is alive
                elif train_info.startswith(b'LT'):
                    if addr[0] == "172.22.156.202":
                        sock_ML.sendto(('LTACK '+ worker.last_work + ' ' + str(worker.ID)).encode(FORMAT), ("172.22.156.202", 8012))
                    sock_ML.sendto(('LTACK '+ worker.last_work + ' ' + str(worker.ID)).encode(FORMAT), ("172.22.158.202", 8012))
                


'''Worker object that process all the inference'''
class Worker:
    def __init__(self, ID, type, batch_size=1):
        '''
            ID: str, Job ID that this worker assigned to
            type: str, Job type, T for translation or V for vision
            batch_size: int, batch size
            workDone: list, task finished so far
            taskQ: list of task to be done
            last_work: str, the task this worker is working on or have just finished
            model_Lang: language model
            model_Vis: vision model
        '''
        self.ID = ID
        if type == 'T':
            self.type = 'Language'
        else:
            self.type = 'Vision'
        self.batch_size = batch_size
        self.taskQ = []
        self.workDone = []
        self._stopEvent = threading.Event()
        self.last_work = ''
        self.sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', 8007))
        self.model_Lang = {'tokenizer': MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-en-CELTIC"),
                        'model': MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-en-CELTIC")}
        self.model_Vis = {'featureExtractor': ViTFeatureExtractor.from_pretrained("google/vit-base-patch16-224"),
                        'model': ViTForImageClassification.from_pretrained("google/vit-base-patch16-224")}
        print('Worker for Job '+str(ID)+' created!')
    
    '''Inference function for language task'''
    def inferLang(self, src_text):
        translated = self.model_Lang['model'].generate(**self.model_Lang['tokenizer'](src_text, return_tensors="pt", truncation=True,max_length = 16))
        tgt_text = [self.model_Lang['tokenizer'].decode(t, skip_special_tokens=True) for t in translated]
        return ' '.join(tgt_text)
    
    '''Inference function for vision task'''
    def inferVis(self, src_img):
        img = Image.open(requests.get(src_img, stream=True).raw)
        inputs = self.model_Vis['featureExtractor'](img, return_tensors="pt")

        with torch.no_grad():
            logits = self.model_Vis['model'](**inputs).logits

        predicted_label = logits.argmax(-1).item()
        return self.model_Vis['model'].config.id2label[predicted_label]
    
    '''Get file from SDFS,  do inference on it, and save results to files'''
    def work(self):
        print('Worker for Job '+str(self.ID)+' start working!')
        while True:
            if self.type == 'Language':
                infer = self.inferLang
            elif self.type == 'Vision':
                infer = self.inferVis
            if self._stopEvent.isSet():
                print('Job terminating!')
                break
            if not self.taskQ:
                continue
            filename = self.taskQ.pop(0)
            self.last_work = filename
            while True:
                exitCode = callSDFS('RR', filename, filename)
                if exitCode == 1:
                    break
                else:
                    time.sleep(0.5)

            with open(filename, 'r') as in_f, \
                open(filename+'_inf', 'w') as out_f:
                for line in in_f:
                    try:
                        data = infer(line.strip())
                    except:
                        data = 'Broken Data!'
                    out_f.write(data+'\n')
                #print(f'Processed file: {filename}, used model {"T" if infer == self.inferLang else "V"}')
            self.workDone.append(filename)
            self.sock.sendto(b'GETMLMAS 8007', ('127.0.0.1', 6019))
            MLNum, _ = self.sock.recvfrom(1024)
            MLNum = int(MLNum)
            self.sock.sendto(b'GETMEM 8007', ('127.0.0.1', 5004))
            mem_list, _ = self.sock.recvfrom(1024)
            mem_list = {i.split(' ')[0]:(i.split(' ')[1],i.split(' ')[2]) for i in mem_list.decode('utf-8').split(',')}
            if MLNum == 0:
                self.sock.sendto(('TF '+filename+'_inf '+str(self.ID)).encode(FORMAT), (mem_list[str(MLNum)][0], 8006))
            #New, send it to the StandByCoordinator
            self.sock.sendto(('TF '+filename+'_inf '+str(self.ID)).encode(FORMAT), ("172.22.158.202", 8006))

    '''Add one task to this worker'''
    def addTask(self, task):
        self.taskQ.append(task)
    
    '''Change job of this worker'''
    def changeJob(self, id, type):#new
        self.ID = id
        if type == 'T':
            self.type = 'Language'
        else:
            self.type = 'Vision'

    '''get information of this worker'''
    def getInfo(self):
        return {'ID': self.ID, 'type':self.type, 
                'batch_size':self.batch_size, 'workDone': self.workDone}
        
    '''Set stop event, put all inference results into SDFS'''
    def stop(self):
        print('Stopping worker...')
        for file in self.workDone:
            while True:
                exitCode = callSDFS('WR', file+'_inf', file+'_inf')
                if exitCode == 1:
                    break
                else:
                    print('Waiting to write...')
                    time.sleep(0.5)
        self._stopEvent.set()
        self.sock.close()
    
    '''Start inference'''
    def run(self):
        threading.Thread(target=self.work).start()

'''Helper to handle C4 commands'''
def C4_handler():
    sock_C4 = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock_C4.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock_C4.bind(('0.0.0.0', 8017))
    while True:
        request, addr = sock_C4.recvfrom(4096)
        if request.startswith(b'C4'):
            filename = request.decode(FORMAT).split(' ')[1]
            with open(filename, 'r') as f:
                data = f.read()
                sock_C4.sendto(data.encode(FORMAT), (addr[0], 8016))
    
if MACHINENUM >= 1:
    threading.Thread(target=instructionConsumer).start()
    threading.Thread(target=C4_handler).start()
    

    

        

                




