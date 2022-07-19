# Map Journal events to Hadoop Sink Demo
## Description

The project demonstrates Hazelcast Platform APIs for sinking events to unsecured local file or kerberos remote HDFS It runs stand-alone(embedded) as java application, or can be submitted to Hazelcast cluster...
	
```
	{HZ_HOME}/bin/hz-cli submit --targets=myPOCHzCluster@localhost:5701 mapjournal-to-hadoop-sink-0.0.1-SNAPSHOT.jar
```