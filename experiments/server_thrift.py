from flask import Flask, jsonify
from flask import render_template, request, url_for

import json
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

conf = SparkConf()\
    .setAppName("PySpark Cassandra Test")\
    .set("spark.driver.allowMultipleContexts", "true")

conf.set("spark.scheduler.mode", "FAIR")

sc = SparkContext("local[*]", "User Approval Aggregates Daily Basis", conf=conf)
all_tables = ["api_events", "transactions"]
sqlContext = SQLContext(sc)
for table in all_tables:
    sqlContext.sql("""CREATE TEMPORARY TABLE %s \
                              USING org.apache.spark.sql.cassandra \
                              OPTIONS ( table "%s", \
                                        keyspace "simpl_events_production", \
                                        cluster "simpl_cassandra_production_cluster", \
                                        pushdown "true") \
                          """ % (table, table))
app = Flask(__name__)

def toCSVLine(data):
  return '|~|'.join(str(d) for d in data)

@app.route('/')
def form():
    return render_template('form_submit.html')

@app.route('/spark/', methods=['POST'])
def spark():
    query = request.form['query']
    # query_elements = query.split(' ')
    # table_name = query_elements[query_elements.index('FROM')+1]
    try:
        df = sqlContext.sql(query)
        output = df.toPandas().to_html()
    except Exception as e:
        print(e)
    return render_template('form_action.html', query=query, output=output)

if __name__ == '__main__':
  app.run()
