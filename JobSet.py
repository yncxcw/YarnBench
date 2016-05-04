#!/bin/python


from threading import Thread

class JobSet:


    def __init__(self):
        self.job_set = []
        self.job_thread_set = []
        self.job_all_finished = False


    def add_job(self,job):
        self.job_set.append(job)
        job_thread = Thread(None,job.run_job,None,())
        self.job_thread_set.append(job_thread)
        job_thread.start()


    def get_job_all_finisjed(self):
        return self.job_all_finished


    def wait_to_completed(self):
        for job_thread in self.job_thread_set:
            print "try to join"
            job_thread.join()
        self.job_all_finished = True
        


