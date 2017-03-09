from flask import Flask, jsonify
from flask import render_template, request, url_for

import json
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import thread

conf = SparkConf()\
    .setAppName("PySpark Cassandra Test")\
    .set("spark.cassandra.connection.host", "192.168.56.101")\
    .set("spark.cassandra.auth.username", "cassandra")\
    .set("spark.cassandra.auth.password", "cassandra")\
    .set("spark.driver.allowMultipleContexts", "true")

conf.set("spark.scheduler.mode", "FAIR")

sc = SparkContext("local[*]", "User Approval Aggregates Daily Basis", conf=conf)
all_tables = ["api_events", "transactions"]
sqlContext = SQLContext(sc)
KEYSPACE = "simpl_events_production"
for table in all_tables:
    sqlContext.sql("""CREATE TEMPORARY TABLE %s \
                              USING org.apache.spark.sql.cassandra \
                              OPTIONS ( table "%s", \
                                        keyspace "%s", \
                                        cluster "simpl_cassandra_production_cluster", \
                                        pushdown "true") \
                          """ % (table, table, KEYSPACE))

app = Flask(__name__)
app.config['DEBUG'] = True
app.config['PROPAGATE_EXCEPTIONS'] = True

def toCSVLine(data):
  return '|~|'.join(str(d) for d in data)

def executeQuery(query, in_json=False):
    try:
        df = sqlContext.sql(query)
        if in_json is True:
            output = df.toPandas().to_json()
        else:
            output = df.toPandas().to_html()
    except Exception as e:
        print(e)
        output = e
    return output

@app.route('/')
def form():
    return render_template('form_submit.html')

@app.route('/spark/', methods=['POST'])
def spark():
    query = request.form['query']
    output = executeQuery(query)
    return render_template('form_action.html', query=query, output=output)

@app.route('/list_tables/', methods=['GET'])
def list_tables():
    query = 'SHOW TABLES'
    return executeQuery(query, in_json=True)

@app.route('/list_columns', methods=['GET'])
def list_columns():
    table = request.args.get('table')
    query = 'DESCRIBE '+table
    return executeQuery(query, in_json=True)

@app.route('/get_keyspace', methods=['GET'])
def get_keyspace():
    return KEYSPACE

if __name__ == '__main__':
    app.run(threaded=True)
# app.run(processes=3)