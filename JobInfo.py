#!/bin/python

PROGRESS ="progress"
CONTAINER="runningContainers"
MB       ="allocatedMB"
VCORE    ="allocatedVCores"
STATE    ="state"
FINISH   ="finishedTime"
START    ="startedTime"
ELAPSE   ="elapsedTime"

class JobInfo:

    def __init__(self,job_id):
        self.job_id      = job_id
        self.job_name    = None
        self.start_time  = 0
        self.finish_time = 0
        self.run_time    = 0
        self.state       = None
        self.finish      = False
        self.statics     ={}

    def monitor(self,job_dict):
        elapse_time = int(dict_read[ELAPSE])
        ##record start time
        if self.start_time is None:
            self.start_time = int(dict_read[START])
        ##record run_time if applicable
        if (self.state == "ACCEPTED" or self.state =="SUBMITTED") and (dict_read[STATE]=="RUNNING"):
            self.run_time = self.start_time + elapse_time 
        ##record state
        self.state = dict_read[STATE]
        ##record progress with elapse time
        if self.staticse.get(PROGRESS) is None:
            self.staticse[PROGRESS] = {}
        self.staticse[PROGRESS][elapse_time]=float(dict_read[PROGRESS])
        ##record container with elapse time
        if self.staticse.get(CONTAINER) is None:
            self.staticse[CONTAINER] = {}
        self.staticse[CONTAINER][elapse_time]=int(dict_read[CONTAINER])
        ##record memory with elapse time
        if self.staticse.get(MB) is None:
            self.staticse[MB] = {}
        self.staticse[MB][elapse_time]=int(dict_read[MB])
        ##record cores with elapse time
        if self.staticse.get(VCORE) is None:
            self.staticse[VCORE] = {}
        self.staticse[VCORE][elapse_time]=int(dict_read[VCORE])
        ##record finish time
        if dict_read[STATE] == "FINISHED":
            self.finish = True
            self.job_finish_time=int(dict_read[FINISH])
        pass
	 

        
