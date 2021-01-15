#!/usr/bin/python
# coding: utf-8
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import paho.mqtt.client as paho
import logging
import time


class MQTTHandler(object):
    # Atributos 'privados'
    _logger = logging.getLogger('root')
    _client = None
    _client_id = 'xbee2-simulator'
    _clean_session = False
    _auto_connect = True

    # Configuración por defecto
    host = '127.0.0.1'
    port = 1883
    keepalive = 60
    qos = 0
    retain = False
    default_topic_pattern = "{address}/{port}"

    ssv = {}
    set_will = False

    def __init__(self, client_id, clean_session, auto_connect):
        # Atributos
        self._client_id = client_id
        self._clean_session = clean_session
        self._auto_connect = auto_connect

        # Inicialización del cliente
        self._client = paho.Client(
            self._client_id, clean_session=self._clean_session)

        # Conectar callbacks
        self._logger.debug(self._client.on_connect)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._logger.debug(self._client.on_connect)

    def connect(self):
        self._logger.debug(
            "Conectando al broker en %s:%s", self.host, self.port)
        ret = self._client.connect(self.host, self.port, self.keepalive)
        return ret

    def disconnect(self):
        self._logger.debug("Desconectando del broker")
        return self._client.disconnect()

    def publish(self, address, port, value):
        topic = self.default_topic_pattern.format(address=address, port=port)
        return self._client.publish(topic, str(value), self.qos, self.retain)

    def _on_connect(self, client, userdata, flags, rc):
        """ Callback de conexión MQTT """
        if rc == 0:
            self._logger.info(
                "Conectado al broker en %s:%s", self.host, self.port)
        else:
            self._logger.error("Falla al conectar con el broker (rc %d)", rc)
            self._reconnect()

    def _on_disconnect(self, mosq, obj, rc):
        if rc == 1:
            self._logger.error(
                "Desconexión del broker %s:%s", self.host, self.port)
            self._reconnect()

    def _reconnect(self):
        crc = 1
        while (self._auto_connect and crc):
            self._logger.info("Reconectando en 10 segundos")
            time.sleep(10)
            crc = self.connect()

    def loop(self):
        self._client.loop()
