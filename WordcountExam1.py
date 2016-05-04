#!/bin/python
import time
from RunHadoop import RunHadoop
from ParseConfigure import ParseConfigure
from HadoopDeploy import HadoopDeploy
from JobRecorder import JobRecorder
from JobRecorder import HadoopJobRecorder
from JobSet import JobSet
import os
import random
import time
HADOOP_HOME=os.environ["HADOOP_HOME"]
#SPARK_HOME =os.environ["SPARK_HOME"]

HADOOP_EXAMPLE=HADOOP_HOME+"/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar"

YARN_USER="jassion"



def make_wordcount(runHadoop):
    
    input_path ="/data"
    output_path="/output"+str(random.randint(0,1000))
    job = HadoopJobRecorder(job_home=HADOOP_HOME,
                            job_user=YARN_USER,
                            job_jar=HADOOP_EXAMPLE,
                            job_exe="wordcount",
                            job_input=input_path,
                            job_output=output_path,
                            runHadoop=runHadoop)
    return job
     



if __name__=="__main__":
       
    result_file = open("./result","w+") 		 
    jobs = JobSet()	
    runHadoop=RunHadoop(HADOOP_HOME)
    parseConfigure=ParseConfigure(HADOOP_HOME)
    hadoopDeploy=HadoopDeploy(HADOOP_HOME)
    parseConfigure.read_all()

    ##stop clusters
    #runHadoop.stop_clusters()
    ##reconfigure configure
    #hadoopDeploy.deploy_etc()
    ##start clusters
    #runHadoop.start_clusters()
    #time.sleep(70)

    	
    print "start"
    i=0
    #while  i  < 1:
    job1=make_wordcount(runHadoop)
    jobs.add_job(job1)
    time.sleep(20)
    job2=make_wordcount(runHadoop)
    jobs.add_job(job2)
    time.sleep(20)
    job3=make_wordcount(runHadoop)
    jobs.add_job(job3)
    time.sleep(20)
    


    print "begin execution"
    jobs.wait_to_completed()


    print "finish"

    

