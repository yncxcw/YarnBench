#!/bin/python


from threading import Thread
import time
class JobSet:


    def __init__(self):
        self.job_set = {}
        self.job_processes= []
        self.job_all_finished = False


    def add_jobs(self,jobs):
        for job in jobs:
            job_process = job.run_job()
            self.job_processes.append(job_process)
            self.job_set[job.current_id]  = job



    def get_job_all_finished(self):
        pass
        
    def wait_to_complete(self):
        for process in self.job_processes:
            while process.poll() is None:
                time.sleep(2)
        
