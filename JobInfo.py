#!/bin/python

PROGRESS ="progress"
CONTAINER="runningContainers"
MB       ="allocatedMB"
VCORE    ="allocatedVCores"
FINALSTATUS="finalStatus"
STATE    ="state"
FINISH   ="finishedTime"
START    ="startedTime"
ELAPSE   ="elapsedTime"
JOBNAME  ="name"


class JobInfo:

    def __init__(self,job_id):
        self.job_id      = job_id
        self.job_name    = None
        self.start_time  = 0
        self.finish_time = 0
        self.run_time    = 0
        self.state       = None
        self.finish      = False
        self.finalStatus = None
        self.statics     ={}

    def monitor(self,dict_read):
        elapse_time = int(dict_read[ELAPSE])
        ##record start time
        if self.start_time == 0:
            self.start_time = int(dict_read[START])

        if self.job_name   == None:
            self.job_name   = dict_read[JOBNAME]

        ##record run_time if applicable
        if (self.state == "ACCEPTED" or self.state =="SUBMITTED") and (dict_read[STATE]=="RUNNING"):
            self.run_time = self.start_time + elapse_time 
        ##record state
        self.state = dict_read[STATE]
        ##record progress with elapse time
        if self.statics.get(PROGRESS) is None:
            self.statics[PROGRESS] = {}
        self.statics[PROGRESS][elapse_time]=float(dict_read[PROGRESS])
        ##record container with elapse time
        if self.statics.get(CONTAINER) is None:
            self.statics[CONTAINER] = {}
        self.statics[CONTAINER][elapse_time]=int(dict_read[CONTAINER])
        ##record memory with elapse time
        if self.statics.get(MB) is None:
            self.statics[MB] = {}
        self.statics[MB][elapse_time]=int(dict_read[MB])
        ##record cores with elapse time
        if self.statics.get(VCORE) is None:
            self.statics[VCORE] = {}
        self.statics[VCORE][elapse_time]=int(dict_read[VCORE])
        ##record finish time
        if dict_read[STATE] == "FINISHED" or dict_read[STATE] == "KILLED":
            self.finalStatus=dict_read[FINALSTATUS]
            self.finish = True
            self.finish_time=int(dict_read[FINISH])
        pass
	 

        
