from contextlib import contextmanager

from rabbit.pub_sub import connection_producer, get_channel


@contextmanager
def create_producer(host: str, exchange: str):
    open_connection = connection_producer(host)

    with open_connection() as connection:
        channel = get_channel(connection)

        channel.exchange_declare(exchange=exchange, exchange_type='fanout')

        def sender(message: str):
            channel.basic_publish(exchange=exchange, routing_key='', body=message)
            print(f'Message "{message}" was sent!')

        try:
            yield sender
        finally:
            pass


if __name__ == '__main__':
    with create_producer("localhost", "log") as produces:
        for n in range(1, 11):
            produces(f'Log of application {n}')
