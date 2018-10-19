from contextlib import contextmanager
from time import sleep

from rabbit.simple_queue import connection_producer, get_channel


@contextmanager
def create_receiver(host: str, queue: str, callback):
    open_connection = connection_producer(host)

    with open_connection() as connection:
        current_channel = get_channel(connection)

        current_channel.queue_declare(queue=queue)

        try:
            current_channel.basic_consume(callback, queue='hello')
            yield current_channel
        finally:
            pass


def process_message(channel, method, properties, body):
    print(f'Received message "{body}"')
    print('Counting dots in message...')
    sleep(body.count(b'.'))
    print('Done')

    print(f'Delivery tag {method.delivery_tag}')

    channel.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':

    with create_receiver("localhost", "hello", process_message) as channel:
        print('Waiting for message. To exit press CTRL+C')
        channel.start_consuming()
