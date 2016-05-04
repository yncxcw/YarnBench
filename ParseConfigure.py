#!/bin/python
from xml.etree.ElementTree import ElementTree

class ParseConfigure:

    ##hadoop home directory
    HADOOP_HOME=""
    ##hdfs-site directory
    hdfs_site=""
    hdfs_tree=ElementTree()
    ##core-site direcory
    core_site=""
    core_tree=ElementTree()
    ##yarn-site direcotry
    yarn_site=""
    yarn_tree=ElementTree()
    ##mapred-site directory
    mapred_site=""
    mapred_tree=ElementTree()

    def __init__(self,hadoop_home):
        self.HADOOP_HOME=hadoop_home
        self.hdfs_site=self.HADOOP_HOME+"/etc/hadoop/hdfs-site.xml"
        self.core_site=self.HADOOP_HOME+"/etc/hadoop/core-site.xml"
        self.yarn_site=self.HADOOP_HOME+"/etc/hadoop/yarn-site.xml"
        self.mapred_site=self.HADOOP_HOME+"/etc/hadoop/mapred-site.xml"

    def read_all(self):
        self.hdfs_tree.parse(self.hdfs_site)
        self.core_tree.parse(self.core_site)
        self.yarn_tree.parse(self.yarn_site)
	self.mapred_tree.parse(self.mapred_site)


    def write_mapred(self):
	self.mapred_tree.write(self.mapred_site,encoding="utf-8")
    def write_hdfs(self):
        self.hdfs_tree.write(self.hdfs_site,encoding="utf-8") 
    def write_core(self):
        self.core_tree.write(self.core_site,encoding="utf-8") 
    def write_yarn(self):
        self.yarn_tree.write(self.yarn_site,encoding="utf-8") 

    def configure_mapred(self,key,value):
	    self.configure_node(self.mapred_tree,key,value)
	    self.write_mapred()
    def configure_hdfs(self,key,value):
        self.configure_node(self.hdfs_tree,key,value)
        self.write_hdfs()
    def configure_core(self,key,value):
        self.configure_node(self.core_tree,key,value)
        self.write_core()
    def configure_yarn(self,key,value):
        self.configure_node(self.yarn_tree,key,value)
        self.write_yarn()
    def configure_node(self,tree,key,value):
	    property_nodes = tree.getroot().findall("property")
 	if len(property_nodes)==0:
	    print "load property error"
	    return		
        for property_node in tree.findall("property"):
            if property_node.find("name").text==key:
                property_node.find("value").text=value
            else:
                continue      
