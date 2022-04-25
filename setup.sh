#! /bin/sh
hadoop fs -rm -r /user/dharrington/wordconfcount/output
hadoop fs -rm -r /user/dharrington/wordconfcount/output2
javac -classpath /home/hdoop/hadoop-3.3.1/share/hadoop/common/hadoop-common-3.3.1.jar:/home/hdoop/hadoop-3.3.1/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.1.jar:/home/hdoop/hadoop-3.3.1/share/hadoop/common/lib/commons-cli-1.2.jar -d wordConf_classes WordConfCount.java
jar -cvf wordcount.jar -C wordConf_classes/ .
