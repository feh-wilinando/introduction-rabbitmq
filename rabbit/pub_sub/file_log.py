from contextlib import contextmanager

from rabbit.pub_sub import connection_producer, get_channel


@contextmanager
def create_receiver(host: str, exchange: str, callback):
    open_connection = connection_producer(host)

    with open_connection() as connection:
        current_channel = get_channel(connection)

        current_channel.exchange_declare(exchange=exchange, exchange_type='fanout')

        result = current_channel.queue_declare(exclusive=True)

        queue_name = result.method.queue

        current_channel.queue_bind(exchange=exchange, queue=queue_name)

        try:
            current_channel.basic_consume(callback, queue=queue_name)
            yield current_channel
        finally:
            pass


was_printed = False


def process_message(channel, method, properties, body):

    global was_printed

    if not was_printed:
        print(f'channel {channel}')
        print(f'method {method}')
        print(f'properties {properties}')
        was_printed = True

    print(f'Received message "{body}"')

    channel.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    with create_receiver("localhost", "log", process_message) as channel:
        print('Waiting for message. To exit press CTRL+C')
        channel.start_consuming()
