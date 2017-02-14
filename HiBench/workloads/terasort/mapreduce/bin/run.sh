#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

workload_folder=`dirname "$0"`
workload_folder=`cd "$workload_folder"; pwd`
workload_root=${workload_folder}/../..
. "${workload_root}/../../bin/functions/load-bench-config.sh"

enter_bench HadoopTerasort ${workload_root} ${workload_folder}
show_bannar start

rmr-hdfs $OUTPUT_HDFS || true

if [ $# -eq 1 ]
then
    OUTPUT_HDFS=$1
fi

if [ $# -eq 2 ]
then
    OUTPUT_HDFS=$1
    QUEUE_NAME=$2
fi


SIZE=`dir_size $INPUT_HDFS`
START_TIME=`timestamp`

DOCKER_IMAGE_KEY="yarn.nodemanager.docker-container-executor.image-name"
DOCKER_IMAGE_VALUE="sequenceiq/hadoop-docker:2.4.1"
run-hadoop-job ${HADOOP_EXAMPLES_JAR} terasort  -D mapreduce.job.queuename=${QUEUE_NAME}  -D${REDUCER_CONFIG_NAME}=60  -Dmapreduce.map.env="$DOCKER_IMAGE_KEY=$DOCKER_IMAGE_VALUE" -Dmapreduce.reduce.env="$DOCKER_IMAGE_KEY=$DOCKER_IMAGE_VALUE"  -Dyarn.app.mapreduce.am.env="$DOCKER_IMAGE_KEY=$DOCKER_IMAGE_VALUE"  ${INPUT_HDFS} ${OUTPUT_HDFS} 


#run-hadoop-job ${HADOOP_EXAMPLES_JAR} terasort -D${REDUCER_CONFIG_NAME}=${NUM_REDS} ${INPUT_HDFS} ${OUTPUT_HDFS} 

END_TIME=`timestamp`

gen_report ${START_TIME} ${END_TIME} ${SIZE}
show_bannar finish
leave_bench

# run bench
#$HADOOP_EXECUTABLE jar $HADOOP_EXAMPLES_JAR terasort -D $CONFIG_REDUCER_NUMBER=$NUM_REDS $INPUT_HDFS $OUTPUT_HDFS
