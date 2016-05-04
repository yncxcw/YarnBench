#!/bin/python
import subprocess
import time

class HadoopDeploy:

    HADOOP_HOME=""	
    SLAVES= ""
    ETC   = ""
    SHARE = ""

    def __init__(self,hadoop_home):
	self.HADOOP_HOME=hadoop_home
	self.ETC        =self.HADOOP_HOME+"/etc" 
	self.SLAVES     =self.ETC+"/hadoop/slaves"
	self.SHARE      =self.HADOOP_HOME+"/share" 	

    def readin_slaves(self):
	files = open(self.SLAVES)
	slaves= []
	while 1:
	    line = files.readline()
	    if not line:
		break
	    slaves.append(line.strip("\n"))
	return slaves


    def deploy_hadoop(self):
	slaves = readin_slaves()
	for slave in slaves:
	    executeStr="scp -r "+self.HADOOP_HOME+" "+slave+":"+self.HADOOP_HOME
	    try:
	        subprocess.call(executeStr,shell=True)
	    except:
	        print "deploy etc failed"	

    def deploy_etc(self):
	slaves = self.readin_slaves()
	for slave in slaves:
	    executeStr="scp -r "+self.ETC+" "+slave+":"+self.HADOOP_HOME
	    try:
	        subprocess.call(executeStr,shell=True)
	    except:
	        print "deploy etc failed"	
	
    def deploy_share():
	slaves = readin_slaves()
	for slave in slaves:
	    executeStr="scp -r "+self.SHARE+" "+slave+":"+self.HADOOP_HOME
	    try:
	        subprocess.call(executeStr,shell=True)
	    except:
	        print "deploy etc failed"	

