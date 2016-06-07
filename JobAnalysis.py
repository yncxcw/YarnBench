#!/bin/python
import time


class JobAnalysis:


    ##mapping from queue to job
    def __init__(job_infos):
        self.job_infos = job_infos
        self.log = open("./job.log")

    def analysis(self):
        ##we devide jobs into different queues
        for queue in self.job_infos.keys():
            self.log.write("=========queue: "+qeueu+"=======")
            ##mapping from id to jobinfo
            jobs = job_infos.get(queue)
            job_list = []
            ##convert into jobinfo list
            for job in jobs.values():
                job_list.append(job)
            ##sort list by submit time
            job_list.sort(key=operator.attrgetter('start_time'))
            ##print to log
            for job in job_list:
                total_time = job.finish_time - job.start_time
                run_time   = job.finish_time - job.run_time
                self.log.write(job.job_id+"  "+total_time+"  "+run_time)
        self.log.close()

