#!/bin/python
import time
import ConfUtils

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

class QueueMonitor:

    def __init__(self,conf):
        self.conf  = conf
        self.url   = conf.get("hadoop.url")+"/ws/v1/cluster/scheduler"


    def monitor_queue(self):
        pass

    @staticmethod
    def get_scheduler_type(conf):
        url = conf.get("hadoop.url")+"/ws/v1/cluster/scheduler"
        dict_read = ConfUtils.read_json_url(url)
        scheduler_type = dict_read["scheduler"]["schedulerInfo"]["type"]
        return scheduler_type

class FifoQueueMonitor(QueueMonitor):
    
    def __init__(self,conf):
        QueueMonitor.__init__(self,conf)
        self.properties = {}

    def monitor_queue(self):
        dict_read = ConfUtils.read_json_url(self.url)
        scheduler_type = dict_read["scheduler"]["schedulerInfo"]["type"]
        if scheduler_type != "fifoScheduler":
            ##TODO
            print "only support fifo scheduler"
            return
        root_queue = dict_read["scheduler"]["schedulerInfo"]
        self.properties[USENOCAP] = root_queue[USENOCAP]
        self.properties[AVANOCAP] = root_queue[AVANOCAP]
        self.properties[TOTALCAP] = root_queue[TOTALCAP]


class CapacityQueueMonitor(QueueMonitor):


    def __init__(self,conf):
        QueueMonitor.__init__(self,conf)
        self.queue_properties={}


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
        if self.queue_properties.get(queue_name) is None:
            self.queue_properties[queue_name] = {}

        self.queue_properties[queue_name][ABCP]    = float(this_queue[ABCP]   )     
        self.queue_properties[queue_name][ABMXCP]  = float(this_queue[ABMXCP] )         
        self.queue_properties[queue_name][ABUSE]   = float(this_queue[ABUSE]  )        
        self.queue_properties[queue_name][NMAPP]   = float(this_queue[NMAPP]  )         
        self.queue_properties[queue_name][NMACAPP] = float(this_queue[NMACAPP])     
        self.queue_properties[queue_name][NMPEAPP] = float(this_queue[NMPEAPP])         
        self.queue_properties[queue_name][NMCON]   = float(this_queue[NMCON]  )     
        self.queue_properties[queue_name][USECP]   = float(this_queue[USECP]  )      
     

    
    def traverse_update_queue(self,root_queue):
        child_queues = root_queue.get("queues")
        ##we reach the leaf queue
        if child_queues is None:
            ##we find the leaf queue and update queue
            name = root_queue["queueName"]
            print "queue name",name
            self.update_queue(name,root_queue) 
        ##we traverse its children
        else:
            for queue in child_queues["queue"]:
                self.traverse_update_queue(queue)

                  
    def get_queue_property(self,queue,name):
        if self.queue_properties[queue] is None:
            raise Exception("error trying to get wrong queue")
            return None

        if self.queue_properties[queue][name] is None:
            raise Exception("error trying to get wrong properties")
            return None

        return self.queue_properties[queue][name]
        
        
                     


