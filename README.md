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

Elasticsearch GUI: http://158.85.79.185:9200/_plugin/gui/index.html#/dashboard

Elasticsearch Head: http://158.85.79.185:9200/_plugin/head/

Spark
-----

Use the key checked into this repo, spark and hadoop colocate on the same node. 158.85.79.185 is its IP

ssh -i id_rsa root@158.85.79.185
or simply run: bash>./scripts/remotessh

Then, type in "spark-shell --packages com.databricks:spark-csv_2.11:1.2.0" to begin

We can also use Spark on YARN, which requires to set master parameter. For example, to run spark-shell on YARN, type in "spark-shell --master yarn-client" to begin

We can also run spark job in the jar file, i.e.

spark-submit --packages com.rubiconproject.oss:jchronic:0.2.6,com.databricks:spark-csv_2.11:1.2.0 --class com.projectx.backend.generateColumnMeta --master spark://node1:7077 --executor-memory 4g --num-executors 5 projectx_2.10-1.0.jar

Job Server
---------
1. add jar
curl --data-binary @jobserver_2.10-1.0.jar 158.85.79.185:8090/jars/readvisgraph

2. pre-create spark context
curl -d "" '158.85.79.185:8090/contexts/projectx?num-cpu-cores=1&memory-per-node=512m'

3. run job synchronously
curl -d "input.visgraph_path=/projectx/output/vis_graph" '158.85.79.185:8090/jobs?appName=readvisgraph&classPath=com.projectx.jobserver.readVisGraph&context=projectx&sync=true'

4. delete spark context
curl -X DELETE 158.85.79.185:8090/contexts/projectx

Elastic Search
--------------
1. start elasticsearch on all nodes
salt-ssh '*' cmd.run 'sudo -u elasticsearch /usr/local/elasticsearch/bin/elasticsearch -d -p /usr/local/elasticsearch/pid'

2. kill elasticsearch on all nodes
salt-ssh '*' cmd.run 'kill -9 `cat /usr/local/elasticsearch/pid`'

3. post document to cluster
curl -X POST 'http://158.85.79.185:9200/artist/person' -d '{
        "talent" : 5,
        "best_song_release_year": 2010,
        "best_song_title": "Eenie Meenie"
}'

curl -X POST 'http://158.85.79.185:9200/artist/person' -d '{
        "talent": 90,
        "best_song_release_year": 1970,
        "best_song_title": "Bron-Y-Aur Stomp"
}'

curl -X POST 'http://158.85.79.185:9200/artist/person' -d '{
        "talent": 91,
        "best_song_release_year": 2013,
        "best_song_title": "Little too Late"
}'

4. test out analyzer
curl -X GET 'http://158.85.79.185:9200/artist/_analyze?field=best_song_title' -d 'Black-cats'

5. search through document
curl -X GET 'http://158.85.79.185:9200/artist/person/_search' -d '{ "query": { "bool": { "should": [ { "match": { "best_song_title": "stom" }} ] } } } '

{
    "query": {
        "bool": {
            "should": [
                { "match": {
                    "best_song_title": "stom"
                }}
            ]
        }
    }
}

6. delete index
curl -XDELETE 'http://158.85.79.185:9200/artist/'

7. configure intial setting for index
curl -XPUT 'http://158.85.79.185:9200/artist/' -d '
{
  "settings":{
    "analysis":{
      "analyzer":{
        "autocomplete":{
          "type":"custom",
          "tokenizer":"standard",
          "filter":[ "standard", "lowercase", "stop", "kstem", "autocomplete_filter" ] 
        }
      },
      "filter":{
        "autocomplete_filter":{
          "type":"ngram",
          "min_gram":2,
          "max_gram":20
        }
      }
    },
    "index" : {
        "number_of_shards" : 2,
        "number_of_replicas" : 1
    }
  },
  "mappings": {
     "person": {
		"properties":{
			"best_song_title": {
			    "type": "string",
			    "search_analyzer": "standard",
			    "analyzer": "autocomplete"
			}
		}
	  }
	}
}'

CORS-proxy
----------
to start cors-proxy, CORSPROXY_HOST=158.85.79.185 nohup corsproxy &
add http://158.85.79.185:1337/ as prefix to any url that needs CORS support

i.e. curl -d "input.visgraph_path=/projectx/output/vis_graph" 'http://158.85.79.185:1337/158.85.79.185:8090/jobs?appName=readvisgraph&classPath=com.projectx.jobserver.readVisGraph&context=projectx&sync=true'
