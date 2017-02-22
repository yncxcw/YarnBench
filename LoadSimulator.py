import ConfUtils
import Queue
import matplotlib.pyplot as pl
import numpy as np
import operator

CPU_OVER_COMMITMENT=True

class Metrics:
    
    def __init__(self,total):
        ##cluster utlization
        self.usages=[]
        ##Running apps
        self.runningApps=[]
        ##normalized times
        self.runningTimes={}
        self.total=total


    def generate_cdf(self,datas):
        datas = np.array(datas)
        count,bv=np.histogram(datas,bins=20)
        cdf=np.cumsum(count)
        cdf=cdf/float(count.sum())
        return bv[1:],cdf



    ##normalized time stored here
    def add_runningTimes(self,type,name,time):
        if self.runningTimes.get(type) is None:
            self.runningTimes[type]={}
        if self.runningTimes[type].get(name) is None:
            self.runningTimes[type][name]=[]
        self.runningTimes[type][name].append(time)

    def draw_runningTimes(self):
        ##one type for one figure
        index=0
        for type in self.runningTimes.keys():
            pl.figure(index)
            for name in self.runningTimes[type].keys():
                datas=self.runningTimes[type][name]
                X,Y=self.generate_cdf(datas)
                pl.plot(X,Y,label=name)
            pl.legend()
            index=index+1
        pl.show()
                
    
    def add_usage(self,clock,resource):
        self.usages.append((clock,resource))

    def add_runningApps(self,clock,apps):
        self.runningApps.append((clock,apps))


    def draw_runningApps(self):
        X=[]
        Y=[]
        for item in self.runningApps:
            time = item[0]
            apps = item[1]
            X.append(time)
            Y.append(apps)
        pl.figure()
        pl.plot(X,Y)
        pl.show()
    

    def draw_usage(self):
        X=[]
        Y=[]
        if CPU_OVER_COMMITMENT:
            for item in self.usages:
                time    = item[0]
                resource= item[1]
                usage= 1.0*(self.total.mem-resource.mem)/self.total.mem
                X.append(time)
                Y.append(usage)
            pl.figure()
            pl.plot(X,Y)
            pl.show()
            ##TODO draw code here
        else:
            pass

class Resource:
    
    def __init__(self,cpu,memory):
        self.cpu = cpu
        self.mem =memory

    def __str__(self):
        return "cpu: "+str(self.cpu)+" memory: "+str(self.mem)

    @staticmethod
    def subtract(r1, r2):
        resource = Resource(r1.cpu-r2.cpu,r1.mem-r2.mem)
        return resource


    @staticmethod
    def add(r1, r2):
        resource = Resource(r1.cpu+r2.cpu,r1.mem+r2.mem)
        return resource

    @staticmethod
    def larger(r1, r2):
        resource = Resource.subtract(r1,r2)
        if CPU_OVER_COMMITMENT:
            if resource .mem > 0:
                return True
            else:
                return False
        else:
            if resource.cpu > 0 and resource.mem > 0:
                return True
            else:
                return False
            

    @staticmethod
    def larger0(r):
        return Resource.larger(r,Resource(0,0))
                
    @staticmethod
    def multiplyN(r, N):
        return Resource(r.cpu*N,r.mem*N)

class Job:

    ##representation of job instance
    def __init__(self,type,name,id,submit):
        self.type  = type
        self.name  =name
        self.id    =id
        self.submit=submit
        self.stime =-1
        self.ftime =-1
        self.task  =-1
        self.rtime =-1
        self.tresource =None
        self.state ="None"

    def start(self,time):
        self.state  = "RUN"
        self.stime  = time
        self.ftime  = self.stime+self.rtime

    def finish(self):
        print "finish: ",self.type," ",self.name," ",self.submit," ",self.stime," ",self.ftime," ",1.0*(self.ftime-self.submit)/self.rtime 
        self.state = "FINISH"

    def set_task_resource(self,resource):
        self.tresource = resource
   
    def get_total_resource(self):
        return Resource.multiplyN(self.tresource,self.task)
 
    def set_task(self,task):
        self.task = task 

    def set_runtime(self,time):
        self.rtime = time

class LoadSimulator:

    def __init__(self):
        self.PREFIX_NAME ="cluster"
        self.JOB_ID      =0

        ##global variable, we only have one conf file at any time
        self.conf         = ConfUtils.Configure()
        self.cluster_size = int(self.conf.get(self.PREFIX_NAME+".size")[0])
        self.node_mem     = int(self.conf.get(self.PREFIX_NAME+".node_mem")[0])
        self.node_cpu     = int(self.conf.get(self.PREFIX_NAME+".node_cpu")[0]) 
        self.total_mem    = self.node_mem * self.cluster_size
        self.total_cpu    = self.node_cpu * self.cluster_size
        self.resource      = Resource(self.total_cpu,self.total_mem)
        self.total         = Resource(self.total_cpu,self.total_mem)

        self.metrics       = Metrics(self.total)
        ##the clock th seconds since starts simulation
        self.clock        = 0
        ##queuing queues
        self.queuings     = []
        ##running queues
        self.runnings     = []
        ##next job index to be scheduled
        self.next_job     = 0

        ##keeping mapping between jobs and its resource(cpu,mem) and estimated runtime
        ##first level type to sets
        self.job_set      ={}
        ##only supporting trace generator now ##TODO
        for job_type in self.conf.get("generator.TraceGenerator1.jobs"):
            self.job_set[job_type]={}
            PREFIX_JOBS = job_type+".jobs"
            for job in self.conf.get(PREFIX_JOBS):
                PREFIX_JOB = PREFIX_JOBS+"."+job
                job_cpu = int(self.conf.gets(PREFIX_JOB+".cpu",PREFIX_JOBS+".cpu")[0])
                job_mem = int(self.conf.gets(PREFIX_JOB+".mem",PREFIX_JOBS+".mem")[0])
                job_resource = Resource(job_cpu,job_mem)
                job_task= int(self.conf.gets(PREFIX_JOB+".task",PREFIX_JOBS+".task")[0])
                job_time= int(self.conf.gets(PREFIX_JOB+".time",PREFIX_JOBS+".time")[0])
                self.job_set[job_type][job]=(job_resource,job_task,job_time)

        ##parsing the trace files ##TODO 
        self.ftrace = self.conf.get("generator.TraceGenerator1.ftrace")[0]
        f=open(self.ftrace,"r")
        self.traces=[]
        ##each line is a time
        for line in f.readlines():
            if len(line) <=1:
                continue
            items = line.strip().split()
            if len(items)<=1:
                stime  = int(items[0])
                job   = None
                name = None
            else:
                stime  = int(items[0])
                job   = items[1].split(",")[0]
                name =  items[1].split(",")[1]
            self.traces.append((stime,job,name))

    

    def gen_new_job(self):
        self.JOB_ID = self.JOB_ID + 1
        jid = self.JOB_ID
        type= self.traces[self.next_job][1]
        name= self.traces[self.next_job][2]
        job = Job(type,name,jid,self.clock)
        tres= self.job_set[type][name][0]
        job.set_task_resource(tres)
        task= self.job_set[type][name][1]
        job.set_task(task)
        time= self.job_set[type][name][2]
        job.set_runtime(time)
        return job

    def finish_job(self,fjob):
        fjob.finish()
        rtime=fjob.ftime-fjob.submit
        normalized=1.0*rtime/fjob.rtime
        self.metrics.add_runningTimes(fjob.type,fjob.name,normalized)
        tresource   =fjob.get_total_resource()
        self.resource=Resource.add(self.resource,tresource)
        self.runnings.remove(fjob)

    def start_job(self,sjob,time):
        sjob.start(time)
        tresource   =sjob.get_total_resource()
        self.resource=Resource.subtract(self.resource,tresource)
        self.queuings.remove(sjob)
        self.runnings.append(sjob)

    def submit_job(self,job):
        self.queuings.append(job)
        self.next_job=self.next_job+1 
        
    ##this is naive simulator, only consider the memory
    ##usage as system usage
    def do_simulate(self):
        finish_submit=False
        while(True):
            ##finish execution
            if finish_submit == True and len(self.runnings) == 0:
                break
            ##generate new jobs
            if finish_submit == False:
                while self.clock == self.traces[self.next_job][0]:
                    job = self.gen_new_job()
                    self.submit_job(job)
                    ##TODO replace to log
                    if self.next_job >= len(self.traces):
                        finish_submit = True
                        break
            ##release resource for finished jobs
            to_release=[]
            for job in self.runnings:
                if job.ftime <= self.clock:
                    to_release.append(job)

            for job in to_release:
                self.finish_job(job)
                    
            ##try to schedule jobs in queuings
            while(len(self.queuings) > 0 and Resource.larger0(self.resource)):
                ##try to schedule first job
                job=self.queuings[0]
                if Resource.larger(self.resource,job.get_total_resource()):
                    self.start_job(job,self.clock)
                ##otherwise, we schedule nothing
                else:
                    break
           
            ##update metrics
            self.metrics.add_usage(self.clock,self.resource)
            self.metrics.add_runningApps(self.clock,len(self.runnings)) 
            ##update clock
            self.clock=self.clock+1
        
        print "final time: ", self.clock
        print "final resource: ",self.resource


    def simulate(self):
        self.do_simulate()


if __name__=="__main__":
    simulator = LoadSimulator()
    simulator.simulate()
    simulator.metrics.draw_usage()
    simulator.metrics.draw_runningTimes()
    simulator.metrics.draw_runningApps()
                
                
                                                
        


                 
