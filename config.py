cassandra_connection_config = {
  'host':     '192.168.56.101',
  'username': 'cassandra',
  'password': 'cassandra'
}
cassandra_config = {
  'cluster': 'rootCSSCluster',
  'tables': {
    'api_events': 'simpl_events_production.api_events',
    'transactions': 'simpl_events_production.transactions',
  }
}