# pip install pyhs2
import pyhs2
 
with pyhs2.connect(host='localhost',
                   port=10000,
                   authMechanism="PLAIN") as conn:
    with conn.cursor() as cur:
        #Show databases
        print cur.getDatabases()
 
        #Execute query
        cur.execute("""CREATE TEMPORARY TABLE api_events USING org.apache.spark.sql.cassandra OPTIONS (table "api_events", keyspace "simpl_events_production", cluster "simpl_cassandra_production_cluster", pushdown "true")""")
 
        #Return column info from query
        cur.execute("SELECT DISTINCT event_name FROM api_events")
        print cur.getSchema()
 
        #Fetch table results
        for i in cur.fetch():
            print i

# with pyhs2.connect(host='localhost',
#                    port=10000,
#                    authMechanism="PLAIN",
#                    user='',
#                    password='',
#                    database='default') as conn: