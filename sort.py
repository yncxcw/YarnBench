#!/bin/python
import time
from RunHadoop import RunHadoop
from ParseConfigure import ParseConfigure
from HadoopDeploy import HadoopDeploy
from JobRecorder import JobRecorder

if __name__=="__main__":
   
    #inputPath  = "/test" 
    inputPath  = ["/wiki_10g_8m","/wiki_10g_16m","/wiki_10g_32m","/wiki_10g_64m","/wiki_10g_96m","/wiki_10g_128m"]
    #outputPath which will be deleted very friquently	
    outputPath = "/result"
    #basic configure
    hadoop_home= "/home/cwei/hadoop-2.6.0"
    #open the file to store final result	
    result_file = open("./result","w+") 		 
    jobs = []	
    runHadoop=RunHadoop(hadoop_home)
    parseConfigure=ParseConfigure(hadoop_home)
    hadoopDeploy=HadoopDeploy(hadoop_home)
    #parseConfigure.read_all()

    print"start"
    i = 0
   
    for path in inputPath[5:]: 	 
	while  i  < 3:
	   time.sleep(10) 
           start=time.time()
           runHadoop.run(runHadoop.HADOOP_EXAMPLE_JAR,"wordcount",path,outputPath,"","")
           end = time.time()
           job_time = end - start  
	   job = JobRecorder(hadoop_home,"cwei")
	   job.init_runHadoop(runHadoop)
	   job.set_job_time(job_time)
           job.copy_job_history()
	   jobs.append(job)	     
           runHadoop.HDFSDeletePath(outputPath)
	   i = i + 1
    for job in jobs:
	print >>result_file,job.get_job()
    print "end"

    

