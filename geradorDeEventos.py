#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import logging
import os
import sys
import thread

import pika


# Configurando logger do módulo
logging_level = logging.INFO

console_handler = logging.StreamHandler()
console_handler.setLevel(logging_level)

logger = logging.getLogger(__name__)
logger.setLevel(logging_level)
logger.addHandler(console_handler)

# Diretório completo dos dados da geração de eventos
data_path = "{0}{1}data{1}".format(os.getcwd(), os.sep)

# Listas de eventos
friendships = []
comments = []
likes = []
posts = []

# Arquivos fonte dos eventos
friendshipsFile = open(data_path + "friendships.dat", "r")
commentsFile = open(data_path + "comments.dat", "r")
likesFile = open(data_path + "likes.dat", "r")
postsFile = open(data_path + "posts.dat", "r")


def send_to_queue(friendships_list, comments_list, likes_list, posts_list):
    """
    Envia, se houver, o conteúdo das listas de eventos ao serviço de filas.

    :param friendships_list: lista de eventos 'amizades'
    :param comments_list: lista de eventos 'coment?rios'
    :param likes_list: lista de eventos 'likes/joinhas'
    :param posts_list: lista de eventos 'posts'
    """
    # Escreve todos os eventos em um único arquivo "output" no mesmo diretório
    # dos arquivos fonte dos eventos.
    # output = open(data_path + "output.dat", 'w')

    while True:
        if len(friendships_list) > 0:
            logger.debug("FRIENDSHIP: %s", friendships_list[0].strip('\n'))
            # output.write(friendships_list[0] + "\n")

            channel.basic_publish(exchange='',
                                  routing_key='friendships',
                                  body=friendships_list[0])

            del friendships_list[0]

        if len(comments_list) > 0:
            logger.debug("COMMENT: %s", comments_list[0].strip('\n'))
            # output.write(comments_list[0] + "\n")

            channel.basic_publish(exchange='',
                                  routing_key='comments',
                                  body=comments_list[0])

            del comments_list[0]

        if len(likes_list) > 0:
            logger.debug("LIKE: %s", likes_list[0].strip('\n'))
            # output.write(likes_list[0] + "\n")

            channel.basic_publish(exchange='',
                                  routing_key='likes',
                                  body=likes_list[0])

            del likes_list[0]

        if len(posts_list) > 0:
            logger.debug("POST: %s", posts_list[0].strip('\n'))
            # output.write(posts_list[0] + "\n")

            channel.basic_publish(exchange='',
                                  routing_key='posts',
                                  body=posts_list[0])

            del posts_list[0]

    # output.close()


def getDateTimeFrom(string):
    """
    Cria um datetime.datetime a partir do string fornecido como argumento.

    Formato do string:
        YYYY-MM-DD'T'HH:MM:SS.mmm

    onde 'mmm' são os microsegundos

    :param string: timestamp no formato <YYYY-MM-DD'T'HH:MM:SS.mmm>
    :return: objeto datetime a partir do timestamp fornecido
    """
    return datetime.datetime.strptime(string, "%Y-%m-%dT%H:%M:%S.%f")


def main(argv):
    now = datetime.datetime.now()
    postDate = postsFile.readline()

    mydatetime = datetime.datetime.strptime(postDate.split("+")[0],
                                            "%Y-%m-%dT%H:%M:%S.%f")

    if len(argv) < 1:
        speedFactor = input(
            "Digite o fator de velocidade por segundo em segundos: ")
    else:
        speedFactor = int(sys.argv[1])

    logger.debug(postDate)

    postDate = postsFile.readline()
    likeDate = likesFile.readline()
    commentDate = commentsFile.readline()
    friendshipDate = friendshipsFile.readline()

    # TODO: Garantir que as mensagens sejam enviadas na sequência correta
    thread.start_new_thread(send_to_queue,
                            (friendships, comments, likes, posts,))

    logger.info("\n--GERADOR INICIADO--\nProcessando eventos...")
    while True:
        newTime = datetime.datetime.now()
        delta = datetime.datetime.now() - now

        if delta.total_seconds() > 1:
            mydatetime += datetime.timedelta(seconds=1 * speedFactor)

            logger.debug("THIS IS THE FAKE TIME NOW: %s", str(mydatetime))
            logger.debug("THIS IS THE REAL TIME NOW: %s\n", str(now))

            now = newTime

            # while (getDateTimeFrom(postDate.split("+")[0]) - mydatetime).total_seconds() <= 0:
            #     # print "POST: " + postDate[:-1]
            #     posts.append(postDate.strip("\n"))
            #     postDate = postsFile.readline()

            while postDate != '':
                line_datetime = getDateTimeFrom(postDate.split("+")[0])
                time_to_next = (line_datetime - mydatetime).total_seconds()

                if time_to_next > 0:
                    break

                posts.append(postDate.strip("\n"))
                postDate = postsFile.readline()

            # while (getDateTimeFrom(likeDate.split("+")[0]) - mydatetime).total_seconds() <= 0:
            #     # print "LIKE: " + likeDate[:-1]
            #     likes.append(likeDate.strip("\n"))
            #     likeDate = likesFile.readline()

            while likeDate != '':
                line_datetime = getDateTimeFrom(likeDate.split("+")[0])
                time_to_next = (line_datetime - mydatetime).total_seconds()

                if time_to_next > 0:
                    break

                likes.append(likeDate.strip("\n"))
                likeDate = likesFile.readline()

            # while (getDateTimeFrom(commentDate.split("+")[0]) - mydatetime).total_seconds() <= 0:
            #     # print "COMMENT: " + commentDate[:-1]
            #     comments.append(commentDate.strip("\n"))
            #     commentDate = commentsFile.readline()

            while commentDate != '':
                line_datetime = getDateTimeFrom(commentDate.split("+")[0])
                time_to_next = (line_datetime - mydatetime).total_seconds()

                if time_to_next > 0:
                    break

                comments.append(commentDate.strip("\n"))
                commentDate = commentsFile.readline()

            # while (getDateTimeFrom(friendshipDate.split("+")[0]) - mydatetime).total_seconds() <= 0:
            #     # print "FRIENDSHIP: " + friendshipDate[:-1]
            #     friendships.append(friendshipDate.strip("\n"))
            #     friendshipDate = friendshipsFile.readline()

            while friendshipDate != '':
                line_datetime = getDateTimeFrom(friendshipDate.split("+")[0])
                time_to_next = (line_datetime - mydatetime).total_seconds()

                if time_to_next > 0:
                    break

                friendships.append(friendshipDate.strip("\n"))
                friendshipDate = friendshipsFile.readline()

if __name__ == "__main__":
    credentials = pika.PlainCredentials('dcb', 'ayyy')

    # connection_params = pika.ConnectionParameters(
    #     "172.16.206.18", 5672, "psd_vhost", credentials
    # )

    parameters = pika.ConnectionParameters(
        "192.168.25.3", 5672, "psd_vhost", credentials
    )

    connection = pika.BlockingConnection(parameters)

    channel = connection.channel()
    channel.queue_declare(queue="friendships")
    channel.queue_declare(queue="posts")
    channel.queue_declare(queue="comments")
    channel.queue_declare(queue="likes")

    try:
        logger.debug("DATA PATH:\n%s\n", data_path)
        main(sys.argv[1:])
    except KeyboardInterrupt:
        logger.debug("\n--Interrupção de teclado--")

    logger.info("\n--GERADOR FINALIZADO--")
