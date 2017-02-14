#!/bin/python
import time
import ConfUtils
from JobInfo import JobInfo
from  threading import Thread
from Analysis import Analysis,AnalysisList,JobAnalysis,CapacityQueueAnalysis
##for capacity scheduler

ABCP   ="absoluteCapacity"      
ABMXCP ="absoluteMaxCapacity"   
ABUSE  ="absoluteUsedCapacity"  
NMAPP  ="numApplications"       
NMACAPP="numActiveApplications"
NMPEAPP="numPendingApplications"
NMCON  ="numContainers"         
USECP  ="usedCapacity"          


##for fifo scheduler

USENOCAP="usedNodeCapacity"
AVANOCAP="availNodeCapacity"
TOTALCAP="totalNodeCapacity"

class Monitor(Thread):

    def __init__(self,conf):
        Thread.__init__(self)
        self.conf   = conf
        self.start_time=str(int(time.time()*1000))
        self.url    = conf.get("hadoop.url")[0]+"/ws/v1/cluster/scheduler"
        self.job_url= conf.get("hadoop.url")[0]+"/ws/v1/cluster/apps?startedTimeBegin="+self.start_time
        ##mapping from the queue to finished jobs
        self.job_infos={}
        ##record running job id
        self.running  =set()
        ##record finish  job id
        self.finish   =set()
        ##if the working thread is running 
        self.is_running  =False
        ##queue info
        self.queue_info={}
        ##submit info
        self.submit_info = {}

    ##return funning job_dicts
    def get_job_dicts(self):
        dict_read = ConfUtils.read_json_url(self.job_url)
        if dict_read is None:
            print "error dict_read"
            return None
        ##no job has been submitted yet
        if dict_read["apps"] is None:
            return None
        return dict_read["apps"]["app"]

    def start(self):
        self.is_running = True
        Thread.start(self)

    def run(self):
        while self.is_running:
            self.monitor_jobs()
            self.monitor_queue()
            ##sleep for 2 seconds
            time.sleep(2)
    
    def analysis(self):
        analysis_list = AnalysisList()
        ##analysis jobs
        job_analy = JobAnalysis(self.job_infos)
        analysis_list.add(job_analy)
        ##analysis queue(TODO currently we only supportes capacity queue)
        queue_analy=CapacityQueueAnalysis(self.queue_info)
        analysis_list.add(queue_analy)
        ##DO analysis here
        analysis_list.analysis()


    def stop(self):
        self.is_running = False

    ##to record the submit jobs
    def monitor_submot(queue,nums):
        pass


    def monitor_jobs(self):
        job_dicts = self.get_job_dicts()
        if job_dicts is None:
            return
        for job_dict in job_dicts:
            id    = job_dict["id"]
            queue = job_dict["queue"]
            ## we ignore, just continue
            if id in self.finish:
                continue
            ##not finish yet
            elif id in self.running:
                job = self.job_infos[queue].get(id)
                job.monitor(job_dict)
                ##if finished, we remove it from running to finish
                if job.finish is True:
                    self.running.remove(id)
                    self.finish.add(id)
                else:
                    pass
            ##it's a new job
            else:
                if self.job_infos.get(queue) is None:
                    self.job_infos[queue] = {}
                self.job_infos[queue][id] = JobInfo(id)
                self.running.add(id)
                self.job_infos[queue][id].monitor(job_dict)                
        pass
    def monitor_queue(self):
        pass

    @staticmethod
    def get_scheduler_type(conf):
        url = conf.get("hadoop.url")[0]+"/ws/v1/cluster/scheduler"
        dict_read = ConfUtils.read_json_url(url)
        scheduler_type = dict_read["scheduler"]["schedulerInfo"]["type"]
        return scheduler_type

class FifoQueueMonitor(Monitor):
    
    def __init__(self,conf):
        Monitor.__init__(self,conf)

    def monitor_queue(self):
        dict_read = ConfUtils.read_json_url(self.url)
        scheduler_type = dict_read["scheduler"]["schedulerInfo"]["type"]
        if scheduler_type != "fifoScheduler":
            ##TODO
            print "only support fifo scheduler"
            return
        root_queue = dict_read["scheduler"]["schedulerInfo"]
        self.queue_info[USENOCAP] = root_queue[USENOCAP]
        self.queue_info[AVANOCAP] = root_queue[AVANOCAP]
        self.queue_info[TOTALCAP] = root_queue[TOTALCAP]


class CapacityQueueMonitor(Monitor):


    def __init__(self,conf):
        Monitor.__init__(self,conf)
        ##absolute capacity
        self.abcp = 0
        ##absolute max capacity
        self.abmcp= 0


    ##current we only supports capacity schduler
    def monitor_queue(self):
        dict_read = ConfUtils.read_json_url(self.url)
        scheduler_type = dict_read["scheduler"]["schedulerInfo"]["type"]
        if scheduler_type != "capacityScheduler":
            ##TODO
            print "only support capacity scheduler"
            return
        root_queue = dict_read["scheduler"]["schedulerInfo"]
        self.traverse_update_queue(root_queue)
        return

    def update_queue(self,queue_name,this_queue):
        ELAPSE = int(time.time() - ConfUtils.START_TIME)
        if self.queue_info.get(queue_name) is None:
            self.queue_info[queue_name] = {}

        self.queue_info[queue_name][ABCP] = float(this_queue[ABCP])
        self.queue_info[queue_name][ABMXCP] = float(this_queue[ABMXCP])

        if self.queue_info[queue_name].get(ABUSE) is None:
            self.queue_info[queue_name][ABUSE] = {}
        self.queue_info[queue_name][ABUSE][ELAPSE] = float(this_queue[ABUSE])

        if self.queue_info[queue_name].get(NMAPP) is None:
            self.queue_info[queue_name][NMAPP] = {}
        self.queue_info[queue_name][NMAPP][ELAPSE] = float(this_queue[NMAPP])

        if self.queue_info[queue_name].get(NMACAPP) is None:
            self.queue_info[queue_name][NMACAPP] = {}
        self.queue_info[queue_name][NMACAPP][ELAPSE] = float(this_queue[NMACAPP])

        if self.queue_info[queue_name].get(NMPEAPP) is None:
            self.queue_info[queue_name][NMPEAPP] = {}
        self.queue_info[queue_name][NMPEAPP][ELAPSE] = float(this_queue[NMPEAPP])

        if self.queue_info[queue_name].get(NMCON) is None:
            self.queue_info[queue_name][NMCON] = {}
        self.queue_info[queue_name][NMCON][ELAPSE] = float(this_queue[NMCON])

        if self.queue_info[queue_name].get(USECP) is None:
            self.queue_info[queue_name][USECP] = {}
        self.queue_info[queue_name][USECP][ELAPSE] = float(this_queue[USECP])    
     

    
    def traverse_update_queue(self,root_queue):
        child_queues = root_queue.get("queues")
        ##we reach the leaf queue
        if child_queues is None:
            ##we find the leaf queue and update queue
            name = root_queue["queueName"]
            self.update_queue(name,root_queue) 
        ##we traverse its children
        else:
            for queue in child_queues["queue"]:
                self.traverse_update_queue(queue)

                  
    def get_queue_property(self,queue,name):
        if self.queue_info[queue] is None:
            raise Exception("error trying to get wrong queue")
            return None

        if self.queue_info[queue][name] is None:
            raise Exception("error trying to get wrong properties")
            return None

        return self.queue_info[queue][name]
        
        
                     


