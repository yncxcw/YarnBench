#!/bin/python
import time
from RunHadoop import RunHadoop
from ParseConfigure import ParseConfigure
from HadoopDeploy import HadoopDeploy
from JobRecorder import JobRecorder

if __name__=="__main__":
   
    #basic configure
    hadoop_home= "/home/cwei/hadoop-2.6.0"
    #open the file to store final result	
    result_file = open("./result","w+") 		 
    jobs = []	
    runHadoop=RunHadoop(hadoop_home)
    parseConfigure=ParseConfigure(hadoop_home)
    hadoopDeploy=HadoopDeploy(hadoop_home)
    parseConfigure.read_all()

    ##stop clusters
    #runHadoop.stop_clusters()
    ##reconfigure configure
    #hadoopDeploy.deploy_etc()
    ##start clusters
    #runHadoop.start_clusters()
    #time.sleep(70)

    print"start"

    bench_list      = ["wordcount","invertedindex","termvectorperhost","grep","histogram_ratings","histogram_movies","sort"]
    bench_input     = ["/wiki_20g_8m","/wiki_20g_8m","/data_6g_8m","/wiki_20g_8m","/movie_10g_8m","/movie_10g_8m","/sort_10g_8m"]
    bench_output    = "/result"
    ##put before path
    begin_parameter = ["-r 8","-r 10","-r 10","","-r 5","-r 5","-r 10"]
    ##put after path
    end_paramater   = ["","","","123","","",""] 
    i = 0
    for i in range(0,1):
	j = 0	
	while  j  < 3:
   	   time.sleep(30) 
   	   start=time.time()
   	   runHadoop.run(runHadoop.HADOOP_EXAMPLE_JAR,bench_list[i],bench_input[i],bench_output,begin_parameter[i],end_paramater[i])
   	   end = time.time()
   	   job_time = end - start  
   	   job = JobRecorder(hadoop_home,"cwei")
   	   job.init_runHadoop(runHadoop)
   	   job.set_job_time(job_time)
	   job.set_job_propertys("benchmark",bench_list[i])
   	   job.copy_job_history()
   	   jobs.append(job)	     
   	   runHadoop.HDFSDeletePath(bench_output)
	   j = j + 1
    for job in jobs:
	print >>result_file,job.get_job_by_propertys("benchmark")
    print "end"

    

