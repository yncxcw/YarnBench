#!/bin/python
import time
from datetime import datetime
import operator
import os

ABCP   ="absoluteCapacity"      
ABMXCP ="absoluteMaxCapacity"   
ABUSE  ="absoluteUsedCapacity"  
NMAPP  ="numApplications"       
NMACAPP="numActiveApplications"
NMPEAPP="numPendingApplications"
NMCON  ="numContainers"         
USECP  ="usedCapacity"          


class Analysis:
    
    def __init__(self,name):
        self.name = name
        self.path =None

    def set_path(self,path):
        self.path = path
        
    def analysis(self):
        pass

    def writeLog(self,log,line):
        try:
            log.write(line)
        except:
            print "write error message"
        

class AnalysisList:

    def __init__(self):
        ##make dir for this run
        self.path = "./logs"
        if os.path.exists(self.path) is False:
            os.mkdir(self.path)
        if os.path.isdir(self.path) is False:
            ##TODO throw exception here
            print "./logs must be dir"
            return
        time_str  ="-".join(str(datetime.now()).split())
        time_str  =time_str.replace(".","-")
        time_str  =time_str.replace(":","-")
        self.path =self.path+"/"+time_str
        os.mkdir(self.path)
        ##store different analysis tool
        self.analysis_list = []

    def add(self,analysis):
        ##TODO tell if analysis is inhereited from Analysis
        analysis.set_path(self.path)
        self.analysis_list.append(analysis)

    def analysis(self):
        for analysis in self.analysis_list:
            analysis.analysis()


class NodeAnalysis(Analysis):
    
    def __init__(self,node_infos):
        Analysis.__init__(self,"NodeAnalysis")
        self.node_infos=node_infos

    def analysis(self):
        if self.path is None:
            print "error path is None"
            return
        for node_host in self.node_infos.keys():
            node=self.node_infos[node_host]
            time_list=sorted(node.keys())
            log=open(self.path+"/"+node_host+".csv","w")
            log.write("assignedM,assignedC,nodeM,nodeC,containerM,conatinerC\n")
            for time in time_list:
                bundle=node[time]
                self.writeLog(log,str(time)+","+str(bundle[0])+","+str(bundle[1])+"," +str(bundle[2])+"," +str(bundle[3])+","
                         +str(bundle[4])+"," +str(bundle[5])+"\n")
            log.close()


class ClusterAnalysis(Analysis):


    def __init__(self,cluster_infos):
        Analysis.__init__(self,"ClusterAnalysis")
        self.cluster_infos=cluster_infos


    def analysis(self):
        if self.path is None:
            print "error path is None"
            return
        time_list = sorted(self.cluster_infos.keys())   
        log = open(self.path+"/cluster.csv","w")
        for time in time_list:
            bundle=self.cluster_infos[time]
            self.writeLog(log,str(time)+","+str(bundle[0])+","+str(bundle[1])+","+str(bundle[2])+"\n")
        log.close() 
         
        

class JobAnalysis(Analysis):


    ##mapping from queue to job
    def __init__(self,job_infos):
        Analysis.__init__(self,"JobAnalysis")
        self.job_infos = job_infos

    def analysis(self):
        if self.path is None:
            print "error path is none"
            return
        ##we devide jobs into different queues
        for queue in self.job_infos.keys():
            ##each queue has its own job log file
            log = open(self.path+"/job_"+queue+".csv","w")
            ##mapping from id to jobinfo
            jobs = self.job_infos.get(queue)
            job_list = []
            ##convert into jobinfo list
            for job in jobs.values():
                job_list.append(job)
            ##sort list by submit time
            job_list.sort(key=operator.attrgetter('start_time'))
            ##print to log
            for job in job_list:
                queue_time = (job.run_time - job.start_time)/1000
                run_time   = (job.finish_time - job.run_time)/1000
                final_status=job.finalStatus
                name        =job.job_name
                self.writeLog(log,job.job_id+","+name+","+final_status+","+str(queue_time)+","+str(run_time)+"\n")
            log.close()


class CapacityQueueAnalysis(Analysis):
    ##mapping from queue to queue
    def __init__(self,queue_infos):
        Analysis.__init__(self,"JobAnalysis")
        self.queue_infos = queue_infos

    def analysis(self):
        if self.path is None:
            print "error path is none"
            return
        ##iterate for queues
        for queue in self.queue_infos.keys():
            queue_map = self.queue_infos.get(queue)
            ##iterate for property
            for property in queue_map.keys():
                log = open(self.path+"/queue_"+queue+"_"+property+".csv","w")
                time_value = queue_map[property]
                if type(time_value) is dict:
                    key_list   = sorted(time_value.keys())
                    for time in key_list:
                        self.writeLog(log,str(time)+","+str(time_value[time])+"\n")
                elif type(time_value) is list:
                    for value in time_value:
                        self.writeLog(log,str(value)+"\n")
                else:
                    self.writeLog(log,str(time_value)+"\n")
                log.close()





