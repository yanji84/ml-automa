# prometheus

Code name: prometheus
---------------------

Cluster
-------

HDFS Namenode: http://158.85.79.185:50070/dfshealth.html#tab-overview

HDFS Explorer: http://158.85.79.185:50070/explorer.html#/

Spark Master: http://158.85.79.185:8080/

YARN: http://158.85.79.185:8088/cluster

AZKABAN: http://158.85.79.185:18081/index (username:azkaban, password:azkaban)

Spark
-----

Use the key checked into this repo, spark and hadoop colocate on the same node. 158.85.79.185 is its IP

ssh -i id_rsa root@158.85.79.185
or simply run: bash>./scripts/remotessh

Then, type in "spark-shell --packages com.databricks:spark-csv_2.11:1.2.0" to begin

We can also use Spark on YARN, which requires to set master parameter. For example, to run spark-shell on YARN, type in "spark-shell --master yarn-client" to begin

We can also run spark job in the jar file, i.e.

spark-submit --packages com.rubiconproject.oss:jchronic:0.2.6,com.databricks:spark-csv_2.11:1.2.0 --class com.projectx.backend.generateColumnMeta --master spark://node1:7077 --executor-memory 4g --num-executors 5 projectx_2.10-1.0.jar