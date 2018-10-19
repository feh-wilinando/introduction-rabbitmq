from time import sleep

from rabbit.simple_queue import connection_producer, get_channel
from contextlib import contextmanager


@contextmanager
def create_sender(host: str, queue: str):
    open_connection = connection_producer(host)

    with open_connection() as connection:
        channel = get_channel(connection)

        channel.queue_declare(queue=queue)

        def sender(message: str):
            channel.basic_publish(exchange='', routing_key=queue, body=message)
            print(f'Message "{message}" was sent!')

        try:
            yield sender
        finally:
            pass


if __name__ == '__main__':

    with create_sender('localhost', 'hello') as send:

        for n in range(1, 11):
            send(f'{n} - Hello World...')
            sleep(1)
