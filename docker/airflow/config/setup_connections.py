#!/usr/bin/env python3
"""
Script to set up Airflow connections from configuration file.
"""
import os
import json
from airflow import settings
from airflow.models import Connection

# Path to the connections configuration file
CONNECTIONS_FILE = '/opt/airflow/config/connections.json'

def setup_connections():
    """Set up Airflow connections from configuration file."""
    # Check if the connections file exists
    if not os.path.exists(CONNECTIONS_FILE):
        print(f"Connections file {CONNECTIONS_FILE} not found.")
        return

    # Load connections from file
    with open(CONNECTIONS_FILE, 'r') as f:
        connections = json.load(f)

    # Create a session
    session = settings.Session()

    # Add each connection
    for conn_id, conn_config in connections.items():
        # Check if connection already exists
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if existing_conn:
            print(f"Connection {conn_id} already exists.")
            continue

        # Create new connection
        conn = Connection(
            conn_id=conn_id,
            conn_type=conn_config.get('conn_type'),
            host=conn_config.get('host'),
            schema=conn_config.get('schema'),
            login=conn_config.get('login'),
            password=conn_config.get('password'),
            port=conn_config.get('port'),
            extra=json.dumps(conn_config.get('extra', {}))
        )

        # Add to session
        session.add(conn)
        print(f"Added connection {conn_id}.")

    # Commit the session
    session.commit()
    session.close()
    print("Connections setup completed.")

if __name__ == '__main__':
    setup_connections() 