#!/usr/bin/python
# coding: utf-8
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import logging


def setup_custom_logger(name, filename, loglevel):
    """ Configura el logger en base a un nombre de clase, nombre de
    archivo y un nombre para el loglevel."""
    logging.basicConfig(filename=filename, level=getattr(logging, loglevel))

    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, loglevel))
    logger.addHandler(handler)
    return logger
