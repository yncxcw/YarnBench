#!/bin/python
import subprocess
import os

class RunHadoop:


    def __init__(self):
        self.HADOOP_HOME=os.environ["HADOOP_HOME"] 
        self.HADOOP_SBIN=self.HADOOP_HOME+"/sbin"
        self.HADOOP_BIN =self.HADOOP_HOME+"/bin"


        
   
    @staticmethod 
    def start_clusters(self):
        executeStr=self.HADOOP_SBIN+"/start-all.sh"
        try:
            subprocess.call(executeStr,shell=True)
        except:
            print "startup with exception"

    @staticmethod 
    def stop_clusters(self):
        executeStr=self.HADOOP_SBIN+"/stop-all.sh"
        try:
            subprocess.call(executeStr,shell=True)
        except:
            print "stop with exception"

    @staticmethod
    def run(self,jarPath,exeName,inputPath,outputPath,beginParameter,endParameter):
        executeStr=self.HADOOP_BIN+"/hadoop jar"+" "+jarPath+" "+exeName+" "+beginParameter+" "+inputPath+" "+outputPath+" "+endParameter
        try:
            subprocess.call(executeStr,shell=True)
        except:
            print "run with exception"

    @staticmethod
    def runHadoopCommand(self,command,option,inputPath="",outputPath=""):
        executeStr=self.HADOOP_BIN+"/hadoop  "+command+" "+option+" "+inputPath+" "+outputPath
        output=None
        try:
            FNULL=open(os.devnull,'w')
            output=subprocess.check_output(executeStr,shell=True,stdout=FNULL,stderr=subprocess.STDOUT)
        except Exception as exceptMessage:
            print "run command with exception",exceptMessage
        return output

    @staticmethod
    def HDFSLsPath(self,path):
	    return self.runHadoopCommand("dfs","-ls",path,"")

    @staticmethod	
    def HDFSDeletePath(self,path):
        return self.runHadoopCommand("dfs","-rmr",path,"")

    @staticmethod
    def HDFSPutPath(self,localPath,hdfsPath):
        return self.runHadoopCommand("dfs","-put",localPath,hdfsPath)

    @staticmethod
    def HDFSGetPath(self,hdfsPath,localPath):
        return self.runHadoopCommand("dfs","-get",hdfsPath,localPath)


