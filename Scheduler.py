#!/bin/python
import ConfUtils
import Queue
import Generator
import time
import JobRecorder
from JobSet import JobSet
class SchedulerPlan:

    
    def __init__(self):
        ##global variable, we only have one plan at any time
        self.conf = ConfUtils.Configure()
        self.cluster_url = self.conf.get("hadoop.url")[0]+"/ws/v1/cluster/info"

        ##check the cluster is running
        clusterInfo=ConfUtils.read_json_url(self.cluster_url)
        if clusterInfo.get("clusterInfo") is None:
            raise Exception("cluster is not running")         
        ##try to make Queue Monitor objects
        scheduler_type = Queue.QueueMonitor.get_scheduler_type(self.conf)
        if  scheduler_type == "capacityScheduler":
            self.queueMonitor=Queue.CapacityQueueMonitor(self.conf)
        elif scheduler_type == "fifoScheduler":
            self.queueMonitor=Queue.FifoQueueMonitor(self.conf)
        else:
            raise Exception("scheduler is not supported")

        ##monitor queue for the first time
        self.queueMonitor.monitor_queue()
        ##start monitoring thread
        self.queueMonitor.start()

        self.generators = []
        ##try to make generator
        generator_types= []
        generator_types += self.conf.get("generators")

        if len(generator_types) is 0:
            raise Exception("missing generators")

        ##TODO reflection
        for generator_type in generator_types:
            if generator_type == "OrderGenerator":
                generator = Generator.OrderGenerator(self.conf,self.queueMonitor)
                ##TODO log
                print "generator: OderGenerator" 
            elif generator_type == "PoissonGenerator":
                generator = Generator.PoissonGenerator(self.conf,self.queueMonitor)
                ##TODO log
                print "generator: PoissonGenerator"
            elif generator_type == "CapacityGenerator":
                generator = Generaor.CapacityGenerator(self.conf,self.queueMonitor)
                ##TODO log
                print "generator: CapacityGenerator"
            else:
                raise Exception("unknown generator")
            self.generators.append(generator)

        ##get run time
        self.run_time = int(self.conf.get("runtime")[0])
        #assert(self.run_time > 100) 

        ##whole job set
        self.jobs = JobSet()      


    def run(self):

        print "start"
        ##inital job id
        JobRecorder.refresh_job_id(self.conf)
        ##main loop
        generator_exist = False
        while self.run_time > 0 and generator_exist is False:
            for generator in self.generators:
                if generator.exit() is True:
                    generator_exist = True
                    break 
                new_jobs = generator.generate_request()
                ##store new jobs
                if new_jobs is None:
                    continue
                else:
                    self.jobs.add_jobs(new_jobs)
            time.sleep(1)
            self.run_time = self.run_time - 1
        print "submit finished"
        self.jobs.wait_to_complete()
        print "main thread finished"
        ##stop monitoring thread
        self.queueMonitor.stop()
        print "stop monitoring thread"
        self.queueMonitor.analysis()
        print "analysis jobs"

           
                 
        
         
if __name__ =="__main__":

    scheduler_plan = SchedulerPlan()
    scheduler_plan.run()



