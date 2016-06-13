#!/bin/python
import time
import operator

class JobAnalysis:


    ##mapping from queue to job
    def __init__(self,job_infos):
        self.job_infos = job_infos
        self.log = open("./job.log","w")

    def analysis(self):
        ##we devide jobs into different queues
        for queue in self.job_infos.keys():
            self.log.write("=========queue: "+queue+"=======\n")
            ##mapping from id to jobinfo
            jobs = self.job_infos.get(queue)
            job_list = []
            ##convert into jobinfo list
            for job in jobs.values():
                job_list.append(job)
            ##sort list by submit time
            job_list.sort(key=operator.attrgetter('start_time'))
            ##print to log
            for job in job_list:
                queue_time = (job.run_time - job.start_time)/1000
                run_time   = (job.finish_time - job.run_time)/1000
                self.log.write(job.job_id+"  "+str(queue_time)+"  "+str(run_time)+"\n")
        self.log.close()

