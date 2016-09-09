#!/bin/python
import time
from JobSet import JobSet
import sys
import JobRecorder
import ConfUtils
import math
import random

class Generator:

    ##a generator can only submit job one queue
    def __init__(self,prefix,conf,queueMonitor):
        if prefix is not None:
            self.PREFIX_NAME = "generator."+prefix
        else:
            print "prefix is none"
        self.conf           = conf
        if conf.get(self.PREFIX_NAME+".queue") is None:
            self.queue = "default"
        else:
            self.queue = conf.get(self.PREFIX_NAME+".queue")[0]
        ##sync time for each requests
        if conf.get(self.PREFIX_NAME+".sync") is None:    
            self.sync       = 0
        else:
            self.sync       = int(conf.get(self.PREFIX_NAME+".sync")[0])
        self.queueMonitor   = queueMonitor
        self.job_types      =[]
        self.job_types      += conf.get(self.PREFIX_NAME+".jobs")
        self.job_maker_sets = {}
        ##count the number of this jobs
        self.job_count      = 0
        ##record the index of current jobs(hadoop, spark, sparksql)
        self.index          =-1
        ##TODO Reflection
        for job in self.job_types:
            if job == "hadoop":
                job_maker = JobRecorder.HadoopMakeJob(conf,self.queue)                 
            elif job == "spark":
                job_maker = JobRecorder.SparkMakeJob(conf,self.queue)
            elif job == "sparksql":
                job_maker = JobRecorder.SparkSQLMakeJob(conf,self.queue)
            elif job == "hibench":
                job_maker =JobRecorder.HiBenchMakeJob(conf,self.queue)
            else:
                print "error:job type not exits"
            self.job_maker_sets[job] = job_maker
        ##must have at least 1 job type
        assert(len(self.job_types) > 0)
        ##job.ratios could be null
        jobratios = conf.get(self.PREFIX_NAME+".jobs.ratios")
        if jobratios is None:
            self.job_ratios  = [1 for i in range(len(self.job_types))] 
        else:
            self.job_ratios  = map(lambda x:float(x), conf.get(self.PREFIX_NAME+".jobs.ratios"))
        ##read in parameters which we want to update during execution
        self.parameter_service = ConfUtils.ParameterService(conf=self.conf,PREFIX_NAME=self.PREFIX_NAME) 
        ## last time to call generate_request
        self.last        = 0
        pass
       
    ##return true if generate a new request
    def generate_request(self):
        ##we try to update scheduler
        self._update_()
        ##try to generate request
        return self._generate_request_()

    ##if the generator exitst
    def exit(self):
        return False

    ##make a job from job_types 
    def _make_job_(self):
        index = ConfUtils.get_type_ratio(self.job_ratios)
        job_maker = self.job_types[index]
        return self.job_maker_sets[job_maker].make_job(self.index)  
        
    def _update_(self):
        pass

 
    ##iner method 
    def _generage_request_(self):
        pass

        
    
##generate request in order
class OrderGenerator(Generator):

    
    def __init__(self,prefix,conf,queueMonitor):
        Generator.__init__(self,prefix,conf,queueMonitor)
        self.current_job = None
        self.count = 0
        self.index = 0
        self.exist = False

        order = self.conf.get(self.PREFIX_NAME+".order")
        if order[0] is None:
            self.order = False
            return

        if order[0] == "true":
            self.order = True
        else:
            self.order = False

        round = self.conf.get(self.PREFIX_NAME+".round")[0]

        if round is None:
            self.round = 1    
        else:
            self.round = int(round)

        range = self.conf.get(self.PREFIX_NAME+".range")[0]

        if range is None:
            self.range = 1
        else:
            self.range = int(range) 
 

    def _generate_request_(self):
        if self.current_job is None or self.current_job.is_finish() is True:
            job=self._make_job_()
            if job is None:
                return None    
            self.last = time.time()
            self.current_job = job
            self.job_count = self.job_count + 1
            new_jobs=[]
            new_jobs.append(job)
            return new_jobs,0 
        else:
            return None,0
   
    def _make_job_(self):
        if self.order is False:
            return Generator._make_job_(self)
        else:
            if self.count >= self.round:
                self.index = self.index + 1
                self.count = 0

            self.count = self.count + 1
            
            if self.index >= self.range:
                self.exist = True
                return None
            else:
                ##under this condition, we only works for first job
                job_maker = self.job_types[0]
                return self.job_maker_sets[job_maker].make_job(self.index)  
        

    def exit(self):
        return self.exist 
            
         
            
##generate request in Poisson distribution       
class PoissonGenerator(Generator):

   
    def __init__(self,prefix,conf,queueMonitor):
        Generator.__init__(self,prefix,conf,queueMonitor)
        ## how long(s) we need to check if we need to submit a job
        self.interval = self.parameter_service.get_parameter("interval")
        ## means for poisson distribution
        self.mean     = self.parameter_service.get_parameter("mean")
        

    def _update_(self):
        self.interval = self.parameter_service.get_parameter("interval")
        self.mean     = self.parameter_service.get_parameter("mean")


    def _generate_request_(self):
        ##if we reach the interval to schedule
        if time.time() - self.last < self.interval:
            return None,0

        self.last = time.time()
        
        m = self.mean
        p = 1.0
        k = 0
        e = math.exp(-m)
        while p >=e:
            u = random.random()
            p*=u
            k+=1
        k=k-1
        ##static enabled
        k=m
        ##we do nothing
        self.job_count = self.job_count + k
        print "this round generate" ,k, "jobs"
        if k < 1:     
            return None,0
        new_jobs = []
        while k > 0:
            job = self._make_job_()
            new_jobs.append(job)
            k-=1
        return new_jobs,self.sync

##generate request in to match the capacity that user set
class CapacityGenerator(Generator):
    

    def __init__(self,prefix,conf,queueMonitor):
        Generator.__init__(prefix,conf,queueMonitor)
        ## we wait 10s to make our new submition effectively occupy the cluster resource
        self.interval       = 10
        self.usedCapacity   = self.parameter_service.get_parameter("usedCapacity")
 

    def _update_(self):
        self.usedCapacity   = self.parameter_service.get_parameter("usedCapacity")

    def _generate_request_(self):
        ##to minimize overhead, we monitor when needed
        self.queueMonitor.monitor_queue()
        target = int(self.get_queue_property(slef.queue,self.capacity_key))
        ##target achieved
        if target >= self.usedCapacity:
            return None,0
        ##check time interval in case of over conmmitting
        if time.time() - self.last < self.interval:
            return None,0

        ##try to conmmit job 
        job = self._make_job_()
        self.job_count = self.job_count + 1
        self.last = time.time()
        new_jobs = []
        new_jobs.append(job)
        return new_jobs,0 

    pass 
