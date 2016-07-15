#/usr/bin/python

import logging
import urllib2
import json
import random
import time

from operator import itemgetter

global start_time

def _init_everything_():
    start_time = time.time()

class Configure:

    confs={}

    confFile="./conf"

    ##initialize with file name and read in file  
    def __init__(self,confFile=None,confs=None):
        if confFile is not None:
            self.confFile = confFile
        if confs is not None:
            self.confs = confs
        self.initialize()
        
    def initialize(self):
        try:
            file = open(self.confFile,"r")
            lines = file.readlines()
            for line in lines:
                #this line is commen
                if line.startswith("#"):
                    pass
                ##this line does not follow typical configuration
                elif "=" not in line:
                    pass
                else:
                    key   = line.split("=")[0].strip()
                    value = line.split("=")[1].strip()
                    ##if value contains more options
                    value_list = []
                    if ","in value:
                        value = value.split(",")
                        value = list(map(lambda x:x.strip(),value))
                        value_list+=value
                    else:
                        value_list.append(value)
                   
                    self.confs[key] = value_list
           		
        except (IOError,OSError) as error:
	        print "error during initialize configure %s",error
	        return False
        return True		    
	    		
    ## get configuration value corresponding to key
    def get(self,key):
        if self.confs.get(key) is None:
            return None
        try:
            if self.confs[key] is not None:
                return self.confs[key]
            else:
                return None
        except KeyError as error:
	        print "error when try to get by key",error
        return None		

    def addConf(self,key,value=None):
	    self.confs[key] = value


    def get_prefix(self,key_prefix):
        results = []
        for key in self.confs.keys():
            if key.startswith(key_prefix):
                ##we don't want this, because we only want a.b.c.d for prefix a.b.c not a.b.d.d.e
                if len(key.split(".")) - len(key_prefix.split(".")) == 1:
                    results.append(key)
                else:
                    pass
            else:
                continue
        return results
            
        
   
    def printConf(self):
        print (self.confs)
        print (self.confFile)

	
   
class ParameterService:

    def __init__(self,conf,PREFIX_NAME):
        self.conf      = conf
        parameter_keys = self.conf.get_prefix(PREFIX_NAME+".parameters")
        self.run_time  = int(self.conf.get("runtime")[0])
        self.parameter_slice_set = {}
        for parameter_key in parameter_keys:
            initial = float(self.conf.get(parameter_key)[0])
            slice   = self.conf.get(parameter_key+".slice")
            parameter_slice = ParameterSlice(
                                            name       =parameter_key,
                                            value      =initial      ,
                                            slices_new =slice        ,
                                            run_time   =self.run_time
                                            )
            self.parameter_slice_set[parameter_key] = parameter_slice
        

    def get_parameter(self,name):
        find = False
        for name_set in self.parameter_slice_set.keys():
            if name_set.split(".")[-1] == name:
                find = True
                return self.parameter_slice_set[name_set].get_current_value()
        if find == False:
            raise Exception("parameter not found exception") 
        return None  



class ParameterSlice:

    ##slice format should be:
    ##5:0.1, 10,0.2,20:0.3 which means at the 10 perfcent of execution time, values turned to be 5
    def __init__(self,name,value,run_time,slices_new=None):
        assert(name  is not None)
        assert(value is not None)
        assert(run_time > 0)
        self.name      = name
        self.initial   = value
        self.mult_slice= True
        self.slices    = []
        ##which means initial value
        self.current_slice = 0
        self._add_slice_(slices_new)
        self.runtime = run_time
        

    def _add_slice_(self,slices_new):
        if slices_new is None:
            self.mult_slice = False
            return
        for term in slices_new:
            time   = float(term.split(":")[0])
            if time < 0 or time > 1:
                raise Exception("illegale parameter negtive value or larger than 1")
            value = float(term.split(":")[1])
            if value < 0:
                raise Exception("illegale parameter negtive value")
            new_term=(time,value)
            self.slices.append(new_term)
        ini_term =(0,self.initial)
        self.slices.append(ini_term)
        ###sort based on time
        self.slices.sort(key=itemgetter(1))

    def get_current_value(self):
        if self.mult_slice is False:
            return self.initial
        ##compute progress
        progress = (time.time() - start_time) * 1.0 / run_time
        
        if self.current_slice < len(self.slices) - 1 and progress > self.slices[self.current_slice+1][0]:
            print "current slice: ",self.current_slice
            self.current_slice = self.current_slice + 1
        
        return self.slices[self.current_slice][1]
  
 
        
        
        
def read_json_url(url):
        dict_read = {}
        try:
            response  = urllib2.urlopen(url)
            dict_read = json.loads(response.read())
        except Exception as exceptMessage:
            print "read url error",exceptMessage
        finally:
            response.close()
        return dict_read    


def get_type_ratio(ratios):
    assert(type(ratios) is list)
    inc_ratios = []
    sum_ratio  = 0
    sums       =sum(ratios)
    for i in range(len(ratios)):
        sum_ratio = sum_ratio + ratios[i]
        inc_ratios.append(sum_ratio*1.0/sums)

    ##generate a radom 0<x<1
    x = random.random()
    ##check which part it will fall in
    low = 0
    up  = 0
    for i in range(len(inc_ratios)):
        low = up
        up  = inc_ratios[i]
        if x >=low and x < up:
            return i
        else:
            continue
    #if we come to here say x = 1
    return len(inc_ratios) -1 
        
        
         
        
  

##for test only

if __name__=="__main__":

    configure1 = Configure()

    
