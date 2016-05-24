#!/bin/python


from threading import Thread
import time
class JobSet:


    def __init__(self):
        self.job_set = {}
        self.job_thread = []
        self.job_all_finished = False


    def add_jobs(self,jobs):
        for job in jobs:
            job_thread = Thread(None,job.run_job,None,())
            self.job_thread.append(job_thread)
            job_thread.start()
            self.job_set[job.current_id]  = job



    def get_job_all_finished(self):
        pass
        
    def wait_to_complete(self):
        for athread in self.job_thread:
            if athread.isAlive:
                athread.join()        
        pass
        
