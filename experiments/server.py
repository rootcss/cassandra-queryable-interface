from flask import Flask, jsonify
from flask import render_template, request, url_for
from simple_spark_lib import SimpleSparkCassandraWorkflow
from config import cassandra_connection_config, cassandra_config

app = Flask(__name__)
workflow = SimpleSparkCassandraWorkflow(appName="Queryable Cassandra Spark")
workflow.setup(cassandra_connection_config, cassandra_config)

@app.route('/')
def form():
    return render_template('form_submit.html')

@app.route('/spark/', methods=['POST'])
def spark():
    query = request.form['query']
    try:
        df = workflow.process(query="SELECT * FROM api_events LIMIT 10000")
        output = df.toPandas().to_html()
    except Exception as e:
        print(e)
    return render_template('form_action.html', query=query, output=output)

if __name__ == '__main__':
  app.run()