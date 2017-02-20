import ConfUtils

class Resoruce:
    
    def __init__(self,cpu,memory):
        self.cpu = cpu
        self.memory=memory


class Job:

    ##representation of job instance
    def __init__(self,type,name,id,submit):
        self.type  = type
        self.name  =name
        self.id    =id
        self.submit=submit
        self.start =-1
        self.finish=-1

    def set_start_time(time):
        self.start = time

    def set_finish_time(time):
        self.finish= time

class LoadSimulator:

    self.PREFIX_NAME ="cluster"
    self.JOB_ID      =0
    def __init__(self):
        ##global variable, we only have one conf file at any time
        self.conf = ConfUtils.configure()
        self.cluster_size = int(self.conf.get(self.PREFIX_NAME+".size")[0])
        self.node_mem     = int(self.conf.get(self.PREFIX_NAME+".node_mem")[0])
        self.node_cpu     = int(self.conf.get(self.PREFIX_NAME+".node_cpu")[0]) 
        self.total_mem    = self.node_mem * self.cluster_size
        self.total_cpu    = self.node_cpu * self.cluster_size
        self.resouce      = Resource(self.total_cpu,self.mem)
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
        ##only supporting trace generator now
        for job_type in self.conf.get("TraceGenerator.jobs"):
            self.job_set[job_type]={}
            PREFIX_JOBS = job_type+".jobs"
            for job in self.conf.get(PREFIX_JOBS):
                PREFIX_JOB = PREFIX_JOBS+"."+job
                job_cpu = self.conf.get(PREFIX_JOB+".cpu")
                job_mem = self.conf.get(PREFIX_JOB+".mem")
                job_resource = Resource(job_cpu,job_mem)
                job_task= self.conf.get(PREFIX_JOB+".task")
                job_time= self.conf.get(PREFIX_JOB+".time")
                self.job_set[job_type]=(job_resource,task,time)

        ##parsing the trace files
        self.ftrace = self.conf.get(self.PREFIX_NAME+".ftrace")[0]
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
        self.JOB_ID = self_JOB_ID + 1
        jid = gen_job_id()
        type= self.traces[self.next_job][1]
        name= self.traces[self.next_job][2]
        job = Job(type,name,jid,self.clock)
        return job
        
    ##this is naive simulator, only consider the memory
    ##usage as system usage
    def do_simulate():
        while(True):
            ##generate new jobs
            if self.clock == self.traces[self.next_job][0]:
                job = self.gen_new_job()
                self.queuings.append(job)
            ##try to schedule jobs in queuings
                                                
        


                 
