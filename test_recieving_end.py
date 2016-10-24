#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Teste para o stream de dados enviados ao serviço de fila
import sys

import pika

conn_params = pika.ConnectionParameters(host='192.168.25.7')
connection = pika.BlockingConnection(parameters=conn_params)
channel = connection.channel()

exchange_name = 'amq.topic'  

channel.exchange_declare(
    exchange=exchange_name,
    durable=True,
    exchange_type='topic',
)

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(
    exchange=exchange_name, queue=queue_name, routing_key='friendships')

channel.queue_bind(
    exchange=exchange_name, queue=queue_name, routing_key='posts')

channel.queue_bind(
    exchange=exchange_name, queue=queue_name, routing_key='comments')

channel.queue_bind(
    exchange=exchange_name, queue=queue_name, routing_key='likes')

def callback(ch, method, properties, body):
    timestamp = body.split('|')[0]
    print(" [x] %r: %r" % (method.routing_key[:4], timestamp))

try:
    channel.basic_consume(callback, queue=queue_name, no_ack=True)
    print "INICIANDO LEITURA"
    channel.start_consuming()
except KeyboardInterrupt:
    print "\nLEITURA INTERROMPIDA"
    sys.exit(1)

