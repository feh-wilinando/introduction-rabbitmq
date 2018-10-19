from pika import BlockingConnection, ConnectionParameters
from contextlib import contextmanager


def connection_producer(host: str):
    connection_parameters = ConnectionParameters(host)

    @contextmanager
    def create_connection():
        connection = BlockingConnection(connection_parameters)

        try:
            yield connection
        finally:
            connection.close()

    return create_connection


def get_channel(connection: BlockingConnection):
    return connection.channel()