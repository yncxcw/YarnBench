/opt/spark-1.5.2-bin-hadoop2.6/bin/spark-submit  --properties-file /opt/HiBench/report/wordcount/spark/java/conf/sparkbench/spark.conf --class org.apache.spark.examples.JavaWordCount --master yarn-client --executor-cores 4 --executor-memory 3G --driver-memory 4G   /opt/HiBench/src/sparkbench/target/sparkbench-5.0-SNAPSHOT-MR2-spark1.6-jar-with-dependencies.jar hdfs://ym:9000/HiBench/Wordcount/Input hdfs://ym:9000/HiBench/Wordcount/Output

