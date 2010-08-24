#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Copyright 2006 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2006
"""
Common utilities module used by REST services
"""

# import system modules
import logging, logging.handlers

def setSQLAlchemyLogger(hdlr,logLevel):
    # set up logging for SQLAlchemy
    logging.getLogger('sqlalchemy.engine').setLevel(logLevel)
    logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(logLevel)
    logging.getLogger('sqlalchemy.pool').setLevel(logLevel)

    logging.getLogger('sqlalchemy.engine').addHandler(hdlr)
    logging.getLogger('sqlalchemy.orm.unitofwork').addHandler(hdlr)
    logging.getLogger('sqlalchemy.pool').addHandler(hdlr)

def setCherryPyLogger(hdlr,logLevel):
    # set up logging for SQLAlchemy
    logging.getLogger('cherrypy.error').setLevel(logLevel)
    logging.getLogger('cherrypy.access').setLevel(logLevel)

    logging.getLogger('cherrypy.error').addHandler(hdlr)
    logging.getLogger('cherrypy.access').addHandler(hdlr)

