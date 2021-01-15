#!/usr/bin/python
# coding: utf-8
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import pika
import logging
import time


class AMQPHandler(object):
    # Atributos 'privados'
    _logger = logging.getLogger('root')
    _connection = None
    _channel = None
    _auto_connect = None

    # Configuración por defecto
    host = "127.0.0.1"
    measurements_exchange = "measurements"
    events_exchange = "events"
    username = "guest"
    password = "guest"

    def __init__(self, auto_connect=True):
        self._auto_connect = auto_connect

    def connect(self):
        """ Establece la conexión con el broker y declara los exchanges
        configurado """
        self._logger.debug("Conectando al broker AMQP en %s", self.host)
        self._credentials = pika.credentials.PlainCredentials(
                self.username,
                self.password
                )

        self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    credentials=self._credentials
                    )
                )

        self._channel = self._connection.channel()

    def disconnect(self):
        """ Cierra la conexión """
        self._connection.close()

    def publish(self, exchange, body):
        """ Publica un mensaje en el exchange configurado """
        self._logger.debug("AMQP publish in %s: %s", exchange, body)
        try:
            self._channel.basic_publish(
                exchange=exchange, routing_key='', body=body)
        except:
            self.reconnect()

    def reconnect(self):
        """ Intenta reconectar al broker configurado,  reintentando cada
        10 segundos """
        fail = 1
        while (self._auto_connect and fail):
            self._logger.info("Reconectando en 10 segundos")
            time.sleep(10)
            try:
                self.connect()
                fail = 0
            except:
                fail = 1
