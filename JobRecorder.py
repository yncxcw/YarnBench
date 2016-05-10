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

    print "current job id",job_id
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

        self.job_command    =None  
        self.job_id         =None
        self.job_start_time =None
        self.job_submit_time=None
        self.job_finish_time=None
        self.job_history    =None
        self.jar            =None
        self.exe            =None
        self.job_name       =None
        self.job_keyValues  ={}
        self.job_parameters =[]
        self.job_input      =[]
        self.job_output     =None
        self.job_app_server =None
        self.start          =False
        self.finish         =False
        self.queue          =None
        self.LOCAL_JOB_HISTORY="./history"
        self.job_propertys = {}
        ##make local dir to store job history
        if os.path.exists(self.LOCAL_JOB_HISTORY) is False:
            self.mkdir_local_history()
	
    def generate_job_name(self):
        stamp=str(random.randint(0,10000))   
        self.job_name=self.exe+stamp    


    def generate_job_report(self):
        return

    def get_type(self):
        return None


    ##get job id if it's accepted by RM and mark the job "started" 
    def wait_job_start(self):
        id = inc_get_id()
        query_url=self.JOB_SERVER
        ##sleep here untill the app is submited to RM
        time.sleep(10)
        dict_read=ConfUtils.read_json_url(query_url)
        if dict_read is None:
            print "error dict_read"
            return
        ##block here untill we find there are some application are running
        while dict_read["apps"] is None:
            dict_read=ConfUtils.read_json_url(query_url)
            continue
        ##we choose the job whose Startedtime has the minimum defference to the job_submit_time
        final_app = None
        ##unit is s
        minimum   = 1000
        ##wait untill we get the job with job_id = id
        print "waiting for submit",id
        while True:
            found = False
            dict_read=ConfUtils.read_json_url(query_url)
            for app in dict_read["apps"]["app"]:
                if int(app["id"].split("_")[-1]) == id: 
                    final_app = app
                    found = True
                    break
            if found is True:
                break
               
        self.job_id=final_app["id"]
        print "start",self.job_id
        self.job_start_time=final_app[START]
        self.job_queue = final_app["queue"] 
        self.job_app_server=self.JOB_SERVER+"/"+self.job_id
        self.start = True
        pass

    def monitor_job(self):
        if self.job_app_server is None:
            print "error: job app server is null"
            return
        dict_read=ConfUtils.read_json_url(self.job_app_server)["app"]
        elapse_time = int(dict_read[ELAPSE])
        if self.job_propertys.get(PROGRESS) is None:
            self.job_propertys[PROGRESS] = {}
        self.job_propertys[PROGRESS][elapse_time]=float(dict_read[PROGRESS])

        if self.job_propertys.get(CONTAINER) is None:
            self.job_propertys[CONTAINER] = {}
        self.job_propertys[CONTAINER][elapse_time]=int(dict_read[CONTAINER])

        if self.job_propertys.get(MB) is None:
            self.job_propertys[MB] = {}
        self.job_propertys[MB][elapse_time]=int(dict_read[MB])

        if self.job_propertys.get(VCORE) is None:
            self.job_propertys[VCORE] = {}
        self.job_propertys[VCORE][elapse_time]=int(dict_read[VCORE])
        if dict_read[STATE] == "FINISHED":
            self.finish = True
            self.job_finish_time=int(dict_read[FINISH])
        pass
	 
    def mkdir_local_history(self):
	    os.mkdir(self.LOCAL_JOB_HISTORY)

    def remove_local_history(self):
	    os.rmdir(self.LOCAL_JOB_HISTORY)

    def add_parameters(self,parameter):
        self.job_parameters.add(parameter)

    def add_keyvalues(self,key,value):
        self.job_keyValues[key]=value


    def run_job(self):
        run_list = []
        run_list.append(self.JOB_BIN)
        run_list.append(self.job_command)
        run_list.append(self.jar)
        run_list.append(self.exe)
        for parameter in self.job_parameters:
            run_list.append(parameter)
        for key in self.job_keyValues.keys():
            run_list.append(key)
            run_list.append(self.job_keyValues[key])
        for input in self.job_input:
            run_list.append(input)
        run_list.append(self.job_output)

        final_run_list = []
        for run in run_list:
            if run is None:
                continue
            else:
                final_run_list.append(run)
        FNULL=open(os.devnull,'w')
        subprocess.Popen(final_run_list,stdout=FNULL,stderr=subprocess.STDOUT)
        print final_run_list
        self.job_submit_time=time.time()
        while self.finish is False:
            if self.start is False:
                ##if we get job id, then we mark it "started"
                self.wait_job_start()
            else:
                self.monitor_job()
                time.sleep(2)
                continue;
        print "finish",self.job_id
        #RunHadoop.HDFSDeletePath(self.job_output)

    def copy_job_history(self):
        ##get job history lits by hdfs ls
        job_historys = RunHadoop.HDFSLsPath(self.JOB_JOB_HISTORY)
        if job_historys is None:
            print "null hdfs job history path"
            return 1
        else:
            for history in job_historys.strip('\n').split("\n")[-3].strip('\n').split(" ")[-1]:
                if history.endswith(self.JOB_JOB_HISTORY_ENDING) and self.job_id in history:
                    self.job_history=history
                    break
                else:
                    continue 
	        ##copy to local folder
            RunHadoop.HDFSGetPath(self.job_history,self.LOCAL_JOB_HISTORY)

    def set_job_time(self,time):
	    self.job_time = time

    def set_job_propertys(self,key,value):
	    self.job_propertys[key]=value

    def get_job_propertys(self,key):
	    return self.job_propertys[key]
    
    def get_job_by_propertys(self,key):
	    string = self.job_id+"  "+str(self.job_propertys[key])+"  "+str(self.job_time)
	    return string

    def get_job(self):
	    string = self.job_id+" "+str(self.job_time)
	    return string
	    		
		
	
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

    def generate_job_report(self):
        return self.exe, "run: ", (self.job_finish_time - self.job_start_time)


    
class SparkJobRecorder(JobRecorder):

    def __init__(self, conf,job_home,job_user,job_jar,job_exe,job_input=None,job_output=None):
        JobRecorder.__init__(self,job_home,job_user,conf)
        self.jar            =job_jar
        self.exe            =job_exe
        self.job_input      =job_input
        self.job_output     =job_output    
        self.JOB_BIN        =self.JOB_HOME+"/bin/spark-submit"
        self.JOB_JOB_HISTORY="/spark/spark-events/"+self.JOB_USER
        pass 

    def get_type(self):
        return "SPARK"

    def generate_job_report(self):
        return self.exe, "run: ", (self.job_finish_time - self.job_start_time)



class SparkSQLJobRecorder(JobRecorder):

    def __init__(self, conf, job_home,job_user):
        JobRecorder.__init__(self,job_home,job_user,conf)
        self.JOB_BIN         = self.JOB_HOME+"/bin/spark-sql"
        self.JOB_JOB_HISTORY = "/spark/spark-events"+self.JOB_USER
        pass 

    def get_type(self):
        return "SPARK"

    def generate_job_report(self):
        return self.exe, "run: ", (self.job_finish_time - self.job_start_time)


class HiBenchJobRecorder(JobRecorder):

    def __init__(self,conf,job_home,job_user,job_type,job_exe):
        JobRecorder.__init__(self,job_home,job_user,conf)
        assert(job_type=="spark" or job_type=="mapreduce")
        self.job_type = job_type
        self.exe=job_xex
        if job_type == "mapreduce":
            self.JOB_BIN = self.JOB_HOME+"/workloads/"+job_exe+"/"+job_type+"/"+"bin/run.sh"
        else:
            self.JOB_BIN = self.JOB_HOME+"/workloads/"+job_exe+"/"+job_type+"/"+"java/bin/run.sh"


    def get_type(self):
        if self.job_type == "spark":
            return "SPARK"
        else:
            return "MAPREDUCE"

    def generate_job_report(self):
        return self.exe, "run: ", (self.job_finish_time - self.job_start_time)



class MakeJob:

    PREFIX_NAME = None
    
 
    def __init__(self,conf,queue):
        self.conf     = conf
        self.job_conf = {}
        self.job_home =None
        self.job_user =conf.get("user")[0]
        self.queue    =queue
        self.jobs     = []
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

            key_values = {}
            for key_value in keyvalues:
                key  = key_value.split(":")[0].strip()
                value= key_value.split(":")[1].strip()
                key_values[key] = value

            self.job_conf[job]["keyvalues"] = key_values


    def add_parameters(self,name,job):
        if self.job_conf[name]["parameters"] is None:
            return job
        if len(self.job_conf[name]["parameters"]) == 0:
            return job
        for parameter in job_conf[name]["parameters"]:
            job.add_parameters(parameter)

        return job

    

    def add_keyvalues(self,name,job):
        if self.job_conf[name]["keyvalues"] is None:
            return job

        if len(self.job_conf[name]["keyvalues"]) is 0:
            return job 

        for key in self.job_conf[name]["keyvalues"].keys():
            job.add_keyvalues(key,self.job_conf[name]["keyvalues"][key])

        return job
        
    
    def make_job(self,queue):
        pass


class HadoopMakeJob(MakeJob):

    PREFIX_NAME = "hadoop.jobs"
    
    def __init__(self,conf,queue):
        MakeJob.__init__(self,conf)
        self.job_home = self.conf.get("hadoop.home")[0]


    def make_job(self):
        index = ConfUtils.get_type_ratio(self.ratios)
        assert(index >=0 and index < len(self.jobs))
        name   = self.jobs[index]
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

    def make_job(self):
        index = ConfUtils.get_type_ratio(self.ratios)
        assert(index >=0 and index < len(self.jobs))
        name   = self.jobs[index]
        jar    = self.job_conf[name]["jars"]
        exe    = name
        inputs = self.job_conf[name]["inputs"]
    
        if self.job_conf[name]["output"] is not None:
            output = self.job_conf[name]["output"]
        else:
            output = "/output_"+"spark"+exe+"_"+str(random.randint(1,1000)) 

        job = SparkJobRecorder(
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
        job.add_keyvalues("--queue",self.queue)
        return job

class SparkSQLMakeJob(MakeJob):
 
    PREFIX_NAME = "sparksql.jobs"

    def __init__(self,conf,queue):
        MakeJob.__init__(self,conf,queue)
        self.job_home = self.conf.get("spark.home")[0]

    def make_job(self):
        index = ConfUtils.get_type_ratio(self.ratios) 
        assert(index >=0 and index < len(self.jobs))
        name  = self.jobs[index] 
        job = SparkSQLJobRecorder(
                                 job_home = self.job_home,
                                 job_user = self.job_user,
                                 conf     = self.conf
                                 )

        job.exe = name
        self.add_keyvalues(name,job)
        ##add sql file path with "-f"
        sql_path = self.conf[self.PREFIX_NAME+".path"]+name 
        assert(os.path.exists(sql_path))
        job.add_keyvalues("--queue",self.queue)
        job.add_keyvalues("-f",sql_path)
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


    def make_job(self):
        index = ConfUtils.get_type_ratio(self.ratios) 
        assert(index >=0 and index < len(self.jobs))
        name  = self.jobs[index] 
        job   = HiBenchJobRecorder(
                                  job_home = self.job_home,
                                  job_user = self.job_user,
                                  job_type = self.job_types[index],
                                  job_exe  = name,
                                  conf     = self.conf
                                  )
        ##random generate output dic
        job_output = "/output_"+job_exe+"_"+job_type+"_"+str(random.randint(1,1000))
        job.add_parameters(job_output)
        job.add_parameters(self.queue) 
        ##we do not have parameters and keyvalues here
        return job
        


