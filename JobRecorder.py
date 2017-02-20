#!/bin/python

import os
import subprocess
import time
import random
import threading
import ConfUtils


from RunHadoop import RunHadoop 

PROGRESS ="progress"
CONTAINER="runningContainers"
MB       ="allocatedMB"
VCORE    ="allocatedVCores"
STATE    ="state"
FINISH   ="finishedTime"
START    ="startedTime"
ELAPSE   ="elapsedTime"


job_id = 0

lock = threading.Lock()

def refresh_job_id(conf):

    global job_id
    _url_ = conf.get("hadoop.url")[0]+"/ws/v1/cluster/apps"
    dict_read=ConfUtils.read_json_url(_url_)
    value = 0 
    if dict_read["apps"] is None:
        value = 0
    else:
        for app in dict_read["apps"]["app"]:
            if int(app["id"].split("_")[-1]) > value:
                value = int(app["id"].split("_")[-1])

    with lock:
        job_id = value
    print "initial job id",job_id


def inc_get_id():
    global job_id
    with lock:
        job_id = job_id +1

    return job_id

    
class JobRecorder:

    JOB_JOB_HISTORY=""
    JOB_JOB_HISTORY_ENDING=""
    JOB_BIN   =""
    LOCAL_JOB_HISTORY=""	
    


    def __init__(self,job_home,job_user,conf):
        self.conf           =conf
        self.JOB_SERVER     =self.conf.get("hadoop.url")[0]+"/ws/v1/cluster/apps"
        self.JOB_USER       =job_user
        self.JOB_HOME       =job_home

        self.job_process    =None
        self.current_id     =inc_get_id()
        self.job_command    =None  
        self.job_history    =None
        self.jar            =None
        self.exe            =None
        self.job_name       =None
        self.job_keyValues  =[]
        self.job_parameters =[]
        self.job_input      =[]
        self.job_output     =None
        self.queue          =None
        self.LOCAL_JOB_HISTORY="./history"
        ##make local dir to store job history
        if os.path.exists(self.LOCAL_JOB_HISTORY) is False:
            self.mkdir_local_history()
	
    def generate_job_name(self):
        stamp=str(random.randint(0,10000))   
        self.job_name=self.exe+stamp    


    
    def get_type(self):
        return None
    
    def mkdir_local_history(self):
	    os.mkdir(self.LOCAL_JOB_HISTORY)

    def remove_local_history(self):
	    os.rmdir(self.LOCAL_JOB_HISTORY)

    def add_parameters(self,parameter):
        self.job_parameters.append(parameter)

    ##we do not use dic here to preserve the order
    def add_keyvalues(self,key,value):
        self.job_keyValues.append((key,value))


    def is_finish(self):
        if self.job_process.poll() is None:
            return False
        else:
            return True

    def run_job(self):
        print "start job",self.current_id
        run_list = []
        ##append bin(e.g., spark-submit)
        run_list.append(self.JOB_BIN)
        ##append command
        run_list.append(self.job_command)
        run_list.append(self.jar)
        ##append exe e.g., wordcount
        run_list.append(self.exe)
        ##append key values
        for (key,value) in self.job_keyValues:
            if key != "none":
                run_list.append(key)
            if value != "none":
                run_list.append(value)
        for input in self.job_input:
            run_list.append(input)
        if self.job_output != "none":
            run_list.append(self.job_output)
        ##append parameter for certain workload
        for parameter in self.job_parameters:
            run_list.append(parameter)

        final_run_list = []
        for run in run_list:
            if run is None:
                continue
            else:
                final_run_list.append(run)
        print " ".join(final_run_list)
        FNULL=open(os.devnull,'w')
        index = random.randint(1,1000000)
        print index
        #FNULL=open("./test/temp_"+str(index),'w')

        self.job_process = subprocess.Popen(final_run_list,stdout=FNULL,stderr=subprocess.STDOUT) 
        return None
        #RunHadoop.HDFSDeletePath(self.job_output)

    def copy_job_history(self):
        ##get job history lits by hdfs ls
        job_historys = RunHadoop.HDFSLsPath(self.JOB_JOB_HISTORY)
        if job_historys is None:
            print "null hdfs job history path"
            return 1
        else:
            for history in job_historys.strip('\n').split("\n")[-3].strip('\n').split(" ")[-1]:
                if history.endswith(self.JOB_JOB_HISTORY_ENDING) and self.current_id in history:
                    self.job_history=history
                    break
                else:
                    continue 
	        ##copy to local folder
            RunHadoop.HDFSGetPath(self.job_history,self.LOCAL_JOB_HISTORY)

	    		
		
	
class HadoopJobRecorder(JobRecorder):

    def __init__(self, conf,job_home,job_user,job_jar,job_exe,job_input=None,job_output=None):
        JobRecorder.__init__(self,job_home,job_user,conf)
        self.jar                   =job_jar
        self.exe                   =job_exe
        self.job_command           ="jar"
        self.job_input             =job_input 
        self.job_output            =job_output    
        self.JOB_BIN               =self.JOB_HOME+"/bin/hadoop"
        self.JOB_JOB_HISTORY_ENDING="jhist"
        self.JOB_JOB_HISTORY       ="/tmp/hadoop-yarn/staging/history/done_intermediate/"+self.JOB_USER
        pass

    def get_type(self):
        return "MAPREDUCE" 
    
class SparkJobRecorder(JobRecorder):

    def __init__(self, conf,job_home,job_user,job_input=None,job_output=None):
        JobRecorder.__init__(self,job_home,job_user,conf)
        self.job_input      =job_input
        self.job_output     =job_output    
        self.JOB_BIN        =self.JOB_HOME+"/bin/spark-submit"
        self.JOB_JOB_HISTORY="/spark/spark-events/"+self.JOB_USER
        pass 

    def get_type(self):
        return "SPARK"

class SparkSQLJobRecorder(JobRecorder):

    def __init__(self, conf, job_home,job_user):
        JobRecorder.__init__(self,job_home,job_user,conf)
        self.JOB_BIN         = self.JOB_HOME+"/bin/spark-sql"
        self.JOB_JOB_HISTORY = "/spark/spark-events"+self.JOB_USER
        pass 

    def get_type(self):
        return "SPARK"

class HiBenchJobRecorder(JobRecorder):

    def __init__(self,conf,job_home,job_user,job_type,job_exe):
        JobRecorder.__init__(self,job_home,job_user,conf)
        assert(job_type=="spark" or job_type=="mapreduce")
        self.job_type = job_type
        ##may cause error here passing more than 2 parameters to run.sh
        #self.exe=job_exe
        if job_type == "mapreduce":
            self.JOB_BIN = self.JOB_HOME+"/workloads/"+job_exe+"/"+job_type+"/"+"bin/run.sh"
        else:
            self.JOB_BIN = self.JOB_HOME+"/workloads/"+job_exe+"/"+job_type+"/"+"java/bin/run.sh"


    def get_type(self):
        if self.job_type == "spark":
            return "SPARK"
        else:
            return "MAPREDUCE"

    
class MakeJob:

    PREFIX_NAME = None
    
 
    def __init__(self,conf,queue):
        self.conf     = conf
        self.job_conf = {}
        self.job_home =None
        self.job_user =conf.get("user")[0]
        self.queue    =queue
        self.jobs          = []
        if conf.get(self.PREFIX_NAME) is None:
            raise Exception("jobs can not be null")
       
        self.jobs += conf.get(self.PREFIX_NAME)

        if conf.get(self.PREFIX_NAME+".ratios") is None:
            ##equal share
            self.ratios = [1 for i in range(len(self.jobs))]
        else:
            self.ratios = map(lambda x:float(x),conf.get(self.PREFIX_NAME+".ratios"))

        ##we should terminate here if we have exception 
        if len(self.jobs) != len(self.ratios):
            raise Exception("jobs and ratios miss match")

        for job in self.jobs:
            self.job_conf[job] = {}
            ##TODO we can do some check here
            if conf.get(self.PREFIX_NAME+"."+job+".jars") is None:
                self.job_conf[job]["jars"] = ""
            else: 
                self.job_conf[job]["jars"] = conf.get(self.PREFIX_NAME+"."+job+".jars")[0]
           
            self.job_conf[job]["inputs"] = []

            if conf.get(self.PREFIX_NAME+"."+job+".inputs") is None:
                self.job_conf[job]["inputs"] = ""
            else:
                self.job_conf[job]["inputs"] += conf.get(self.PREFIX_NAME+"."+job+".inputs")     

            if conf.get(self.PREFIX_NAME+"."+job+".output") is None: 
                self.job_conf[job]["output"] = None
            else: 
                self.job_conf[job]["output"] = conf.get(self.PREFIX_NAME+"."+job+".output")[0]

            ##hadoop.jobs.wordcount.parameters should override hadoop.jobs.parameters if not null
            parameters = []
            if conf.get(self.PREFIX_NAME+"."+job+".parameters") is not None: 
                parameters += conf.get(self.PREFIX_NAME+"."+job+".parameters")
            elif conf.get(self.PREFIX_NAME+".parameters")  is not None:
                parameters += conf.get(self.PREFIX_NAME+".parameters")
            else:
                pass

            self.job_conf[job]["parameters"] = parameters

            keyvalues = []

            if conf.get(self.PREFIX_NAME+"."+job+".keyvalues") is not None:
                keyvalues += conf.get(self.PREFIX_NAME+"."+job+".keyvalues")
            elif conf.get(self.PREFIX_NAME+".keyvalues")  is not None:
                keyvalues += conf.get(self.PREFIX_NAME+".keyvalues")
            else:
                pass

            self.job_conf[job]["keyvalues"] = keyvalues


    def add_parameters(self,name,job):
        if self.job_conf[name]["parameters"] is None:
            return job
        if len(self.job_conf[name]["parameters"]) == 0:
            return job
        for parameter in self.job_conf[name]["parameters"]:
            job.add_parameters(parameter)

        return job

    

    def add_keyvalues(self,name,job):
        if self.job_conf[name]["keyvalues"] is None:
            return job

        if len(self.job_conf[name]["keyvalues"]) is 0:
            return job 

        for keyvalue in self.job_conf[name]["keyvalues"]:
            key  = keyvalue.split(":")[0].strip()
            value= keyvalue.split(":")[1].strip()
            job.add_keyvalues(key,value)

        return job
        
    
    def make_job(self, job_name):
        pass


class HadoopMakeJob(MakeJob):

    PREFIX_NAME = "hadoop.jobs"
    
    def __init__(self,conf,queue):
        MakeJob.__init__(self,conf)
        self.job_home = self.conf.get("hadoop.home")[0]


    def make_job(self,job_name):
        if job_name == None:
            index = ConfUtils.get_type_ratio(self.ratios)
            assert(index >=0 and index < len(self.jobs))
            name  = self.jobs[index]
        else:
            name = job_name
        jar    = self.job_conf[name]["jars"]
        exe    = name
        inputs = self.job_conf[name]["inputs"]

        if self.job_conf[name]["output"] is not None:
            output = self.job_conf[name]["output"]
        else:
            output = "/output_"+"hadoop"+exe+"_"+str(random.randint(1,1000)) 

        job = HadoopJobRecorder(
                                job_home = self.job_home,
                                job_user = self.job_user,
                                job_jar  = jar          ,
                                job_exe  = name         ,
                                job_input= inputs       ,
                                job_output=output       ,
                                conf      =self.conf
                                )
        self.add_parameters(name,job)
        self.add_keyvalues(name,job)
        job.add_keyvalues("-D","mapreduce.job.queuename="+self.queue) 
        return job

                      
class SparkMakeJob(MakeJob):

    PREFIX_NAME = "spark.jobs"


    def __init__(self,conf,queue):
        MakeJob.__init__(self,conf,queue)
        self.job_home = self.conf.get("spark.home")[0]

    def make_job(self,job_name):
        if job_name is None:
            index = ConfUtils.get_type_ratio(self.ratios)
            assert(index >=0 and index < len(self.jobs))
            name  = self.jobs[index]
        else:
            name  = job_name
        inputs = self.job_conf[name]["inputs"] 
        if self.job_conf[name]["output"] is not None:
            output = self.job_conf[name]["output"]
        else:
            output = "/output_"+"spark_"+str(random.randint(1,1000000)) 

        job = SparkJobRecorder(
                                job_home = self.job_home,
                                job_user = self.job_user,
                                job_input= inputs       ,
                                job_output=output       ,
                                conf      =self.conf
                                )
        self.add_parameters(name,job)
        job.add_keyvalues("--queue",self.queue)
        self.add_keyvalues(name,job)
        return job

class SparkSQLMakeJob(MakeJob):
 
    PREFIX_NAME = "sparksql.jobs"

    def __init__(self,conf,queue):
        MakeJob.__init__(self,conf,queue)
        self.job_home = self.conf.get("spark.home")[0]

    def make_job(self,job_name):
        if job_name is None:
            index = ConfUtils.get_type_ratio(self.ratios)
            assert(index >=0 and index < len(self.jobs))
            name = self.jobs[index]
        else:
            name = job_name
        job = SparkSQLJobRecorder(
                                 job_home = self.job_home,
                                 job_user = self.job_user,
                                 conf     = self.conf
                                 )

        job.exe = name
        sql_path = self.conf.get(self.PREFIX_NAME+".path")[0]+name 
        assert(os.path.exists(sql_path))
        job.add_keyvalues("--queue",self.queue)
        job.add_keyvalues("-f",sql_path)
        self.add_keyvalues(name,job)
        self.add_parameters(name,job)
        return job
        

class HiBenchMakeJob(MakeJob):

    PREFIX_NAME="hibench.jobs"

    def __init__(self,conf,queue):
        MakeJob.__init__(self,conf,queue)
        self.job_home  = self.conf.get("hibench.home")[0]
        types = self.conf.get("hibench.jobs.types")
        if types is None:
            self.job_types = ["mapreduce" for i in range(len(self.jobs))]
        else:
            self.job_types = types


    def make_job(self,job_name):
        if job_name is None:
            index = ConfUtils.get_type_ratio(self.ratios)
            assert(index >=0 and index < len(self.jobs))
            name  = self.jobs[index] 
        else:
            name  = job_name
        job   = HiBenchJobRecorder(
                                  job_home = self.job_home,
                                  job_user = self.job_user,
                                  job_type = self.job_types[index],
                                  job_exe  = name,
                                  conf     = self.conf
                                  )
        ##random generate output dic
        job_output = "/output_"+name+"_"+job.job_type+"_"+str(random.randint(1,100000))
        job.add_parameters(job_output)
        job.add_parameters(self.queue) 
        ##we do not have parameters and keyvalues here
        return job
        
