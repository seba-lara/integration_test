#!/usr/bin/python2
# coding: utf-8
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import csv
import sys
import time
import getopt
import re
import json
import pdb
import logging
from configparser import ConfigParser
import libs.log as log
import csv
from libs.MQTTHandler import MQTTHandler
from libs.AMQPHandler import AMQPHandler
import datetime
from threading import Thread

# Leer versión desde .hg_archival.txt
try:
    archival = open('.hg_archival.txt', 'r')
    archivalstr = archival.read()
except:
    archivalstr = ""

mv = re.search(r"latesttag: ([\w.]+)\nlatesttagdistance: (\w+)", archivalstr)
version = mv.group(1) + "." + mv.group(2) if mv else "0"

__app__ = "csv-enqueuer"
__version__ = version


def usage():
    """ Muestra la ayuda del programa """
    print("""Usage: csv-enqueuer [-c config_file] [-l log_file] -k KEY -f CSVFILE

 -c : especifica el archivo de configuración.
 -d : dummy_info
 -f : archivo csv
 -h : muestra esta ayuda.
 -k : key con la cual se publicará.
 -l : especifica el archivo de logging.
 -s : single_step
 -a : utilizar canal AMQP
    """)

# Parsear opciones de línea de comandos
try:
    opts, args = getopt.getopt(
            sys.argv[1:], "hsac:l:k:f:d:",
            ["help", "single_step", "amqp", "config_file", "log_file",
                "key", "csv_file", "dummy"]
            )
except getopt.GetoptError as err:
    # print help information and exit:
    print(str(err))
    usage()
    sys.exit(2)

config_file = "csv-enqueuer.conf"
log_file = "/tmp/csv-enqueuer.log"
key = 'key'
csv_file = None
dummy_mode = False
single_step = False
amqp_mode = False

for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-c", "--config_file"):
            config_file = a
        elif o in ("-l", "--log_file"):
            log_file = a
        elif o in ("-k", "--key"):
            key = a
        elif o in ("-f", "--csv_file"):
            csv_file = a
        elif o in ("-d", "--dummy"):
            dummy_mode = True
        elif o in ("-s", "--single_step"):
            single_step = True
        elif o in ("-a", "--amqp"):
            amqp_mode = True
        else:
            assert False, "unhandled option"


class CSVEnqueuer():
    _mqtthandler = None
    _smbuffer = {}
    _runner = True
    _amqphandler = None
    _logger = None

    def run(self):

        if(dummy_mode):
            self._run_dummy()
            sys.exit(0)

        try:
            csvfile = open(csv_file, 'rb')
            reader = csv.reader(csvfile)
        except:
            raise

        header = reader.next()

        try:
            self._init_start()
        except:
            print("Error inesperado")
            raise

        if amqp_mode:
            header = [x.replace("values.", "") for x in header]
        else:
            mq = Thread(target=self.mqttLoop)
            mq.start()

        datadic = {}
        for row in reader:
            for col in range(0, len(row)):
                datadic[header[col]] = row[col]
            address = datadic.pop('key', None)
            ts = datadic.pop('timestamp', None)

            if amqp_mode:
                self._amqphandler.publish(
                    self._amqphandler.measurements_exchange,
                    json.dumps({
                        'key': address,
                        'vncp_measurement_version': '1.0.0',
                        'values': self.normalize(datadic)
                        })
                    )
                if(single_step):
                    raw_input("Presione una tecla para enviar otra medición:")
                else:
                    time.sleep(self._sleep)
            else:
                for val in sorted(datadic.keys()):
                    port = val.replace("values.", "")
                    value = datadic[val]
                    if(value != ''):
                        self._mqtthandler.publish(address, port, value)
                if(single_step):
                    raw_input("Presione una tecla para enviar otra medición:")
                else:
                    time.sleep(self._sleep)
        self._runner = False
        time.sleep(3)

    def normalize(self, smbuffer):
            normalized_data = {}

            # en caso a ser firmware previo a 0.3.0
            if('firmware' not in smbuffer):
                #normalized_data['firmware'] = '0.2.0'
                # fórmula que lo traduce de vuelta a vbatt y luego a %
                if('batt_percentage' not in smbuffer):
                    perc_old = int(smbuffer.get("batt"))
                    vbatt = float(14.718*8.4*perc_old/(14*100))
                    bp = (
                        -0.1371625*pow(vbatt, 4) + 3.63025*pow(vbatt, 3) -
                        35.2775*pow(vbatt, 2) + 150.03*vbatt - 236.4)/0.034
                    bp = 0 if bp < 0 else 100 if bp > 100 else bp
                    #normalized_data['batt_percentage'] = bp
                #else:
                    #normalized_data['batt_percentage'] = float(smbuffer['batt_percentage'])
            else:
                fw = smbuffer['firmware']
                #normalized_data['firmware'] = fw
                if('batt_percentage' not in smbuffer):
                    # fórmula que traduce vbatt a batt_percentage
                    vbatt = float(smbuffer['vbatt'])
                    bp = (
                        -0.1371625*pow(vbatt, 4) + 3.63025*pow(vbatt, 3) -
                        35.2775*pow(vbatt, 2) + 150.03*vbatt - 236.4)/0.034
                    bp = 0 if bp < 0 else 100 if bp > 100 else bp
                    #normalized_data['batt_percentage'] = bp
                #else:
                    #normalized_data['batt_percentage'] = float(smbuffer['batt_percentage'])
            # copiar los values de mic y accel al arreglo normalizado
            for data_key in smbuffer.keys():
                if 'batt' not in data_key and 'firmware' not in data_key:
                    try:
                        normalized_data[data_key] = int(smbuffer.get(data_key))
                    except:
                        print(smbuffer.get(data_key))
                        assert("asdad")

            print(normalized_data)
            return normalized_data

    def mqttLoop(self):
        while self._runner:
            self._mqtthandler.loop()
            time.sleep(1)

    def _init_start(self):
        # Configuración
        config = ConfigParser()
        config.read(config_file)

        # Loggear a un archivo
        self._logger = log.setup_custom_logger('root', log_file, config.get(
            "APP", "loglevel"))

        """ Inicializa los handlers """
        # Handlers
        if amqp_mode:
            self._sleep = 0.05
            self._amqphandler = AMQPHandler()
            self._amqphandler.host = config.get('AMQP', 'host')
            self._amqphandler.measurements_exchange = config.get(
                'AMQP', 'measurements_exchange')
            self._amqphandler.events_exchange = config.get(
                'AMQP', 'events_exchange')
            self._amqphandler.username = config.get('AMQP', 'username')
            self._amqphandler.password = config.get('AMQP', 'password')

            self.ttl = datetime.timedelta(
                    milliseconds=config.getint('AMQP', 'ttl')
                    )
            try:
                self._amqphandler.connect()
                self._logger.info("Conectado al broker AMQP")
            except:
                self._logger.error("No se pudo conectar al broker AMQP")
                self._amqphandler.reconnect()
        else:
            self._mqtthandler = MQTTHandler(
                    config.get('MQTT', 'client_id'),
                    config.get('MQTT', 'clean_session'),
                    config.getboolean('MQTT', 'auto_connect'))
            self._mqtthandler.host = config.get('MQTT', 'host')
            self._sleep = float(config.get('APP', 'sleep'))
            self._mqtthandler.port = config.getint('MQTT', 'port')
            self._mqtthandler.keepalive = config.getint('MQTT', 'keepalive')
            self._mqtthandler.qos = config.getint('MQTT', 'qos')
            self._mqtthandler.retain = config.getboolean('MQTT', 'retain')
            self._mqtthandler.default_topic_pattern = config.get(
                'MQTT', 'default_topic_pattern')
            try:
                self._mqtthandler.connect()
                self._logger.info("Conectado al broker MQTT")
            except:
                self._logger.error("No se pudo conectar al broker MQTT")
                self._mqtthandler.reconnect()

    def _run_dummy(self):
        try:
            self._init_start()
        except:
            print("Error inesperado")
            raise
        mq = Thread(target=self.mqttLoop)
        mq.start()
        ports = [
            'mic0', 'mic1', 'mic2', 'mic3', 'mic4', 'mic5', 'mic6', 'mic7',
            'mic8', 'mic9', 'mic10', 'mic11', 'mic12', 'mic13', 'mic14',
            'mic15', 'mic16', 'mic17', 'mic18', 'mic19', 'mic20', 'mic21',
            'mic22', 'mic23', 'mic24', 'mic25', 'mic26', 'mic27', 'mic28',
            'mic29', 'mic30', 'mic31', 'accel0', 'accel1', 'accel2', 'accel3',
            'accel4', 'accel5', 'accel6', 'accel7', 'accel8', 'accel9',
            'accel10', 'accel11', 'accel12', 'accel13', 'accel14', 'accel15',
            'accel16', 'accel17', 'accel18', 'accel19', 'accel20', 'accel21',
            'accel22', 'accel23', 'accel24', 'accel25', 'accel26', 'accel27',
            'accel28', 'accel29', 'accel30', 'accel31', 'batt']
        vals = [
            '0', '0', '0', '0', '0', '0', '0', '0', '0', '231', '269',
            '161', '143', '0', '544', '507', '390', '457', '200', '0', '97',
            '0', '0', '0', '34', '0', '0', '0', '0', '0', '0', '168', '1', '0',
            '0', '0', '0', '147', '194', '274', '11', '229', '413', '48',
            '285', '393', '399', '379', '129', '386', '604', '429', '249',
            '235', '166', '279', '208', '336', '328', '34', '261', '224',
            '205', '171', '69']
        l = 0
        while(self._runner):
            l = l+1
            if(l == 30):
                l = 0
            addr = ('/wsn1/0013a2004%07d' % (l))
            for i in range(0, len(ports)):
                self._mqtthandler.publish(addr, ports[i], vals[i])
            if(single_step):
                raw_input("Presione una tecla para enviar otra medición:")
            else:
                time.sleep(self._sleep)


# Programa principal
if __name__ == "__main__":
    print('{app} v{version} - Tecnología Integral S.A. © 2015'.format(
        app=__app__, version=__version__))

    csvenqueuer = CSVEnqueuer()
    try:
        if(csv_file is not None or dummy_mode):
            print("Para detener, presionar [Ctrl]+c")
            csvenqueuer.run()
            csvenqueuer._runner = False
        else:
            print(
                "Error: Debe incluir un archivo csv," +
                " ejecutar con -h para más opciones")
    except KeyboardInterrupt:
        csvenqueuer._runner = False
        sys.exit(0)
