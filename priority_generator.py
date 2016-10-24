#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import logging
import os
import Queue as queue
import sys
import thread

import pika


# Configurando o Logger do módulo
logging_level = logging.DEBUG

console_handler = logging.StreamHandler()
console_handler.setLevel(logging_level)

logger = logging.getLogger(__name__)
logger.setLevel(logging_level)
logger.addHandler(console_handler)

# Diretório completo dos dados da aplicação
data_path = "{1}{0}data{0}{2}"
data_path = "{1}{0}data_testes{0}{2}"
current_dir = os.getcwd()

friendships_path = data_path.format(os.sep, current_dir, "friendships.dat")
comments_path = data_path.format(os.sep, current_dir, "comments.dat")
likes_path = data_path.format(os.sep, current_dir, "likes.dat")
posts_path = data_path.format(os.sep, current_dir, "posts.dat")

# Tempo do último batch de eventos enviados ao serviço de filas
last_iteration = datetime.datetime.now()
timestamp_format = "%Y-%m-%dT%H:%M:%S.%f"

if len(sys.argv) == 3:
    time_speed_factor = int(sys.argv[2])
    ip_address = sys.argv[1]

else:
    # time_speed_factor = 86400   # 1d/s
    time_speed_factor = 43200   # 0.5d/s
    # ip_address = '172.16.206.18'
    ip_address = '192.168.25.7'

# Configuração do serviço de filas
credentials = pika.PlainCredentials(username='guest', password='guest')
parameters = pika.ConnectionParameters(host=ip_address, port=5672)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
exchange_name = 'amq.topic'

# Fila das mensagens de evento
message_queue = queue.PriorityQueue()

channel.exchange_declare(
    exchange=exchange_name, type='topic', durable=True)

try:
    with open(friendships_path, 'r') as friendships:
        friendships_init_time = friendships.readline().split('+')[0]

    with open(comments_path, 'r') as comments:
        comments_init_time = comments.readline().split('+')[0]

    with open(likes_path) as likes:
        likes_init_time = likes.readline().split('+')[0]

    with open(posts_path) as posts:
        posts_initial_time = posts.readline().split('+')[0]

    initial_timestamp = min(
        friendships_init_time,
        comments_init_time,
        likes_init_time,
        posts_initial_time
    )

    simulated_time = datetime.datetime.strptime(
        initial_timestamp, timestamp_format)

    logger.debug("SIMULATED TIME: %s", simulated_time)

except IOError as e:
    sys.stderr.write("ARQUIVO NÃO ENCONTRADO\n{}".format(str(e)))
    sys.exit(1)


def get_datetime_from(string):
    return datetime.datetime.strptime(string, timestamp_format)


def parse_file(file_path):
    filename = file_path.split(os.sep)[-1]
    event_topic = filename.strip('.dat')
    # event_topic = topic.format(filename.strip('.dat'))

    input_file = open(file_path, 'r')
    line_read = input_file.readline().strip('\n')

    while line_read != '':
        timestamp = line_read.split('+')[0]
        message_queue.put_nowait((timestamp, event_topic, str(line_read)))
        line_read = input_file.readline()

    input_file.close()


def send_to_queue_service():
    global last_iteration
    global simulated_time

    while True:
        time_now = datetime.datetime.now()
        elapsed_seconds = (time_now - last_iteration).total_seconds()

        if elapsed_seconds < 1:
            continue

        last_iteration = time_now
        simulated_time += datetime.timedelta(seconds=1 * time_speed_factor)

        while not message_queue.empty():
            event = message_queue.get_nowait()
            timestamp = event[0]
            event_topic = event[1]
            message = event[1] + '|' + event[2]

            event_time = get_datetime_from(timestamp)
            time_to_next = (event_time - simulated_time).total_seconds()

            if time_to_next > 0:
                logger.debug("NO EVENT")
                break

            logger.debug(
                "SENT: (Topic: %s, Timestamp: %s)", event_topic[:5], timestamp)

            channel.basic_publish(
                exchange=exchange_name, routing_key=event_topic, body=message)


if __name__ == "__main__":
    logger.info("--INICIANDO LEITURA--")

    try:
        friendship_reader = thread.start_new_thread(
            parse_file, (friendships_path,))

        post_reader = thread.start_new_thread(
            parse_file, (posts_path, ))

        comment = thread.start_new_thread(
            parse_file, (comments_path, ))

        like = thread.start_new_thread(
            parse_file, (likes_path, ))

        logger.info("Enviando mensagens...")
        send_to_queue_service()
        logger.info("--MENSAGENS ENVIADAS--")

    except KeyboardInterrupt:
        logger.info("--LEITURA INTERROMPIDA--")
        sys.exit(1)
