from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

conf = SparkConf().setAppName("PySpark Cassandra Test")
conf.set("spark.scheduler.mode", "FAIR")
sc = SparkContext("local[*]", "User Approval Aggregates Daily Basis", conf=conf)
sqlContext = SQLContext(sc)
sqlContext.sql("""CREATE TEMPORARY TABLE api_events USING org.apache.spark.sql.cassandra OPTIONS (table "api_events", keyspace "simpl_events_production", cluster "simpl_cassandra_production_cluster", pushdown "true")""")
sqlContext.sql("SELECT DISTINCT event_name FROM api_events").show()
sc.stop()


# url="jdbc:hive2://thrift-server-url:10000/default;user=[ldap_user];password=[ldap_user_password]"
# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.10:1.5.0-M2 __server.py
