# -*- coding=utf-8 -*-
'''
Created on 2013-5-27

@author: Wu Yinghao
@e-mail: wyh817@gmail.com
@weibo:  weibo.com/wuyinghao

该demo程序主要用来模拟分布式计算中的paxos算法
系统分为三个部分：
Leader：负责所有议案和议案编号的分发，这是一个辅助线程
Proposer：议案的提出者，负责提出议案并等待各个接收者的表决
Acceptor: 议案的表决者，负责接收议案并进行表决

无论是议案的提出者还是表决者，发送数据的时候都有一定的概率（2%）发送失败导致对方接受不到议案或议案的表决内容，用来模拟真实网络中丢包的情况
无论是议案的提出者还是表决者，发送数据的时候都有一定延时，用来模拟真实网络中的数据延迟导致的接收超时情况

该程序只是一个demo模拟，没有任何实际用途，主要用来理解paxos协议

注意：由于都是最后一个启动的Proposer线程拿到最大编号，所以最后都是最后一个线程的议案通过，但是这并不影响paxos协议，因为每次通过的议案虽然
编号是一样的，但是议案的内容每次都是不一样的

为了简单起见，所有参与者默认都是在线的，但是发送数据会有不成功而已，如果需要模拟成有人掉线需要再修改

关于paxos的具体内容请自行搜索
================================================================================================================
运行结果：

从管理者处获取数据...                                                            #从Leader处获取议案，议案编号和表决者列表
从管理者处获取数据...
从管理者处获取数据...
从管理者处获取数据...
proposer0发出了一个决议，内容为:[第一块数据由A进行更新]                              #提出一个议案，【】中是议案的内容
proposer1发出了一个决议，内容为:[第一块数据由B进行更新]
proposer2发出了一个决议，内容为:[第一块数据由C进行更新]
proposer3发出了一个决议，内容为:[第一块数据由D进行更新]
proposer4发出了一个决议，内容为:[第一块数据由E进行更新]
Acceptor19 >>>>> 发送审批失败                                                  #模拟网络发送失败
proposer0 >>>>> 发送决议失败
proposer4 >>>>> 发送决议失败
proposer3的本轮决议[第一块数据由C进行更新]投票结束，同意:17拒绝：3选择:0               #收到的表决内容
proposer3的决议[第一块数据由C进行更新]被否决，停止提议，退出                          #对收到的表决内容分析后提出新的表决
proposer0的本轮决议[第一块数据由A进行更新]投票结束，同意:2拒绝：16选择:0
proposer4的本轮决议[第一块数据由B进行更新]投票结束，同意:19拒绝：0选择:0
proposer3发出了一个决议，内容为:[第一块数据由C进行更新]                              #提出了一个新的表决，等待判决
proposer0的决议[第一块数据由A进行更新]被否决，停止提议，退出                          #决议被否决，该proposer停止提交议案
proposer4发出了一个决议，内容为:[第一块数据由B进行更新]
proposer1的本轮决议[第一块数据由B进行更新]投票结束，同意:10拒绝：9选择:0
proposer0发出了一个决议，内容为:[第一块数据由A进行更新]
proposer1的决议[第一块数据由B进行更新]被否决，停止提议，退出
proposer0 >>>>> 发送决议失败
proposer1发出了一个决议，内容为:[第一块数据由B进行更新]
proposer2的本轮决议[第一块数据由B进行更新]投票结束，同意:17拒绝：3选择:0
proposer0 >>>>> 发送决议失败
proposer2的决议[第一块数据由B进行更新]被否决，停止提议，退出
proposer2发出了一个决议，内容为:[第一块数据由B进行更新]
proposer3 >>>>> 发送决议失败
Acceptor2 >>>>> 发送审批失败
proposer4 >>>>> 发送决议失败
Acceptor18 >>>>> 发送审批失败
proposer0的本轮决议[第一块数据由A进行更新]投票结束，同意:0拒绝：18选择:0
proposer2的本轮决议[第一块数据由B进行更新]投票结束，同意:0拒绝：20选择:0
proposer4的本轮决议[第一块数据由C进行更新]投票结束，同意:1拒绝：0选择:17
proposer3的本轮决议[第一块数据由C进行更新]投票结束，同意:0拒绝：18选择:1
proposer1的本轮决议[第一块数据由B进行更新]投票结束，同意:0拒绝：19选择:0
proposer0的决议[第一块数据由A进行更新]被否决，停止提议，退出
proposer2的决议[第一块数据由B进行更新]被否决，停止提议，退出
proposer4发出了一个决议，内容为:[第一块数据由C进行更新]
proposer3的决议[第一块数据由C进行更新]被否决，停止提议，退出
proposer1的决议[第一块数据由B进行更新]被否决，停止提议，退出
proposer4的本轮决议[第一块数据由C进行更新]投票结束，同意:0拒绝：0选择:20
############# proposer4的决议[第一块数据由C进行更新]被同意，完成决议过程 #############   #决议被通过，模拟完成，每次被通过的议案内容不一样

'''

from Queue import Queue 
import random  
import threading  
import time  
from multiprocessing import Queue
from Queue import Empty


mutex = threading.Lock()


###############################################
#
#辅助函数
#打印信息的辅助函数，避免多线程打印打乱的情况
#
###############################################
def printStr(string):
    mutex.acquire()
    print string
    mutex.release()


###############################################
#
#paxos 决议管理者
#负责所有的决议管理和编号分发
#
###############################################
class Leader(threading.Thread):
    def __init__(self, t_name,          #发起者名称
                 queue_to_leader,       #接收请求的队列
                 queue_to_proposers,    #和proposer通讯的消息队列
                 acceptor_num           #表决者数量，用来生成表决者编号
                 ):       
        threading.Thread.__init__(self, name=t_name)          
        self.queue_recv=queue_to_leader
        self.queue_send_list=queue_to_proposers
        #acceptor编号
        self.acceptor_list=range(0,acceptor_num)
        #议案内容
        self.value_index=0
        self.values=["[第一块数据由A进行更新]",
                     "[第一块数据由B进行更新]",
                     "[第一块数据由C进行更新]",
                     "[第一块数据由D进行更新]",
                     "[第一块数据由E进行更新]",
                     "[第一块数据由F进行更新]"]
        #议案编号
        self.value_num=100

    def run(self):
        while(True):
            #接收请求，分配议案
            var=self.queue_recv.get()
            #请求数据
            if(var["type"]=="request"):
                #接收到数据"
                #随机分配半数以上的acceptors
                acceptors=random.sample(self.acceptor_list, len(self.acceptor_list)+1/2)
                rsp={
                    "value":self.values[self.value_index],  #议案内容
                    "value_num":self.value_num,             #议案编号
                    "acceptors":acceptors                   #表决者编号
                    }
                self.value_num+=1
                self.value_index+=1
                
            #更新接收者列表
            if(var["type"]=="renew"):
                var_list=var["list"]
                for i in var["failure"]:
                    var_list.remove(i)
                    tmp=random.sample(self.acceptor_list,1)
                    while (tmp[0] in var["failure"]):
                        tmp=random.sample(self.acceptor_list,1)
                    var_list.append(tmp[0])
                rsp={
                    "list":var_list
                    }
                    
            self.queue_send_list[var["ID"]].put(rsp)

                



###############################################
#
#paxos 决议发起者
#议案的提出者，负责提出议案并等待各个接收者的表决
#
###############################################
class Proposer(threading.Thread):
    def __init__(self, t_name,          #发起者名称
                 q_to_leader,           #和leader通信的队列
                 queue_from_acceptor,   #和acceptor通讯的消息队列
                 queue_to_acceptors,    #接收消息队列
                 m_num,                 #ID号
                 m_acceptor_num):       #总共的acceptor数量        
        threading.Thread.__init__(self, name=t_name)
        self.queue_to_leader=q_to_leader
        self.queue_recv=queue_from_acceptor
        self.queue_send_list=queue_to_acceptors
        self.num=m_num
        self.reject=0
        self.accept=0
        self.chosen=0
        self.start_propose=False
        self.fail_list=[]

        
    def run(self):
        #从leader那里获取数据
        self.getValueFromLeader()
        lens=len(self.acceptors)
        #给自己发送一个start信号
        start_sig={
                    "type":"start"
                   }
        self.queue_recv.put(start_sig)
        #循环接收消息
        while (True):
            try:
                var=self.queue_recv.get(True,1)
                #接收到消息，准备处理
                self.processMsg(var)
                
            except Empty:
                #没有接受到消息
                if(self.start_propose==True and time.time()-self.time_start > 5):
                    printStr(self.name +"的本轮决议"+self.value+"投票结束，同意:"+str(self.accept)+"拒绝："+str(self.reject) + "选择:"+str(self.chosen))
                    self.start_propose=False
                    if(self.reject>0):
                        printStr(self.name+"的决议"+self.value+"被否决，停止提议，退出")
                    if(self.chosen==len(self.acceptors)):
                        printStr("############# "+self.name+"的决议"+self.value+"被同意，完成决议过程 #############")
                    if (self.accept>0 or
                       (self.chosen<len(self.acceptors) and self.chosen>0 and self.reject==0) or
                       (self.accept==0 and self.chosen==0 and self.reject==0)):
                        self.reject=0
                        self.chosen=0
                        self.accept=0
                        self.sendPropose()                     
                continue
    ###############################################
    #
    #从leader那里获取数据
    #
    ###############################################
    def getValueFromLeader(self):
        
        req={
            "type":"request",
            "ID":self.num  
            }
        printStr("从管理者处获取数据...")
        self.queue_to_leader.put(req)
        info=self.queue_recv.get()
        #准备数据
        self.s_num=info["value_num"]
        self.value=info["value"]
        self.acceptors=info["acceptors"]
        
    ###############################################    
    #
    #处理报文
    #
    ###############################################
    def processMsg(self,var):
        #如果是启动命令，启动程序
        if(var["type"]=="start"):
            self.sendPropose()
        #如果是acceptor过来的报文，解析报文
        if(var["type"]=="accpting"):
            #超时丢弃
            if(time.time()-self.time_start > 5 ):
                printStr("无效报文，丢弃...")
                self.fail_list.append(var["accpetor"])
            else:
                if(var["result"]=="reject"):
                    self.reject+=1
                if(var["result"]=="accept" ):
                    self.accept+=1
                    #修改决议为acceptor建议的决议
                    self.value=var["value"]
                    self.myvar={
                    "type":"proposing",
                    "Vnum":self.s_num,
                    "Value":var["value"],
                    "proposer":self.num
                    }
                        
                if(var["result"]=="chosen"):
                    self.chosen+=1
            
    ###############################################
    #
    #发送议案给表决者
    #
    ###############################################
    def sendPropose(self):
        self.time_start=time.time()
        self.start_propose=True
        time.sleep(1/random.randrange(1,20))
        printStr(self.name +"发出了一个决议，内容为:"+ str(self.value))
        for acceptor in self.acceptors:
            #生成决议，有5%概率发送失败
            if(random.randrange(100) < 98):
                self.myvar={
                         "type":"proposing",
                         "Vnum":self.s_num,
                         "Value":self.value,
                         "proposer":self.num,
                         "time":self.time_start
                         }
                #printStr(self.name + " >>>>>" +str(var))
                self.queue_send_list[acceptor].put(self.myvar)
            else:
                printStr(self.name + " >>>>> 发送决议失败")
            
            time.sleep(1/random.randrange(1,10))  
    




            
###############################################
#
#paxos 决议表决者acceptor
#负责接收proposer的决议并进行表决
#
###############################################

class Acceptor(threading.Thread):
    def __init__(self, t_name, queue_from_proposer,queue_to_proposers,m_num):  
        threading.Thread.__init__(self, name=t_name)  
        self.queue_recv=queue_from_proposer  
        self.queue_to_proposers=queue_to_proposers
        self.num=m_num
        self.values={
                     "last":0,    #最后一次表决的议案编号
                     "value":"",  #最后一次表决的议案的内容
                     "max":0}     #承诺的最低表决议案编号

  
    def run(self):
        while(True):
            try:
                var=self.queue_recv.get(False,1)
                vars=self.processPropose(var)
                #有2%的概率发送失败
                if(random.randrange(100) < 98):
                    self.queue_to_proposers[var["proposer"]].put(vars)
                else:
                    printStr(self.name + " >>>>> 发送审批失败")
            except Empty:
                continue

    ###############################################
    #
    #处理议案提出者提出的决议
    #
    ###############################################
    def processPropose(self,value):
        res={}
        #如果从来没接收过议案，跟新自身议案
        if(0==self.values["max"] and 0==self.values["last"]):
            self.values["max"]=value["Vnum"]
            self.values["last"]=value["Vnum"]
            self.values["value"]=value["Value"]
            res={
                 "type":"accpting",
                 "result":"accept",
                 "last":0,
                 "value":self.values["value"],
                 "accpetor":self.num,
                 "time":value["time"]}
        else:
            #如果收到的议案大于承诺最低表决的议案，同意并告知之前表决结果
            if(self.values["max"] < value["Vnum"]):
                self.values["max"]=value["Vnum"]
                res={
                    "type":"accpting",
                    "result":"accept",
                     "last":self.values["last"],
                     "value":self.values["value"],
                     "accpetor":self.num ,
                     "time":value["time"]}
            else:
                #如果收到的议案等于承诺最低表决的议案，完全同意议案，表决结束
                if(self.values["max"] == value["Vnum"]):
                    
                    self.values["last"]=value["Vnum"]
                    self.values["value"]=value["Value"]
                    res={
                        "type":"accpting",
                        "result":"chosen",
                        "last":self.values["last"],
                        "value":self.values["value"],
                        "accpetor":self.num,
                        "time":value["time"]
                     }
                else:
                    #如果收到的议案小于承诺最低表决的议案，直接拒绝
                    res={
                        "type":"accpting",
                        "result":"reject",
                        "last":self.values["last"],
                        "value":self.values["value"],
                        "accpetor":self.num,
                        "time":value["time"]
                     }
        return res



if __name__ == '__main__':
    acceptors_num=20
    proposers_num=5
    q_to_acceptors=[]
    q_to_proposers=[]
    proposers=[]
    acceptors=[]

    q_leader_to_proposers=[]
    q_to_leader=Queue()
    
    for i in range(0,acceptors_num):
        q_to_acceptors.append(Queue())
        
    for i in range(0,proposers_num):
        q_to_proposers.append(Queue())
        q_leader_to_proposers.append(Queue())


    ld=Leader("Leader",q_to_leader,q_to_proposers,acceptors_num)
    ld.setDaemon(True)
    ld.start()
    
    for i in range(0,proposers_num):
        proposers.append(Proposer("proposer"+str(i),          
                 q_to_leader,
                 q_to_proposers[i],   
                 q_to_acceptors,    
                 i,                 
                 10))
    
    
    for i in range(0,acceptors_num):
        acceptors.append(Acceptor("Acceptor"+str(i),        
                 q_to_acceptors[i],   
                 q_to_proposers,    
                 i))
        
        
    for i in range(0,len(acceptors)):
        acceptors[i].setDaemon(True)
        acceptors[i].start()
        
        
    for i in range(0,len(proposers)):
        proposers[i].setDaemon(True)
        proposers[i].start()
        
 
    

    
    
    
    
    
    
    
