#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902,R0903
"""
Common utilities module used by REST services
"""
__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id: Utils.py,v 1.4 2008/12/19 01:17:10 valya Exp $"
__version__ = "$Revision: 1.4 $"

# import system modules
import time
import types
import logging
import logging.handlers

def getArg(kwargs, key, default):
    '''extract argument from a dict and assign default value if any'''
    arg = default
    if  kwargs.has_key(key):
        try:
            arg = kwargs[key]
            if  type(default) is types.IntType:
                arg = int(arg)
        except ValueError:
            pass
    return arg

def timeGMT(itime):
    '''return GMT time'''
    try:
        return time.strftime("%d %b %Y %H:%M:%S GMT", time.gmtime(itime))
    except ValueError:
        return "Unknown time format, iTime=%s" % itime

def timeGMTshort(itime):
    '''return GMT time in short format'''
    try:
        return time.strftime("%d/%m/%y", time.gmtime(itime))
    except ValueError:
        return "Unknown time format, iTime=%s" % itime

def colorSizeHTMLFormat(i):
    '''return size of the file in colors'''
    size = str(sizeFormat(i))
    # PB are in red
    if  size.find('PB') != -1:
        return size.replace('PB', '<span class="box_red">PB</span>')
    # TB are in blue
    elif size.find('TB') != -1:
        return size.replace('TB', '<span class="box_blue">TB</span>')
    # GB are in block
    # MB are in green
    elif size.find('MB') != -1:
        return size.replace('MB', '<span class="box_green">MB</span>')
    # KB are in lavender
    elif size.find('KB') != -1:
        return size.replace('KB', '<span class="box_lavender">KB</span>')
    else:
        return size
    
def sizeFormat(i):
    """
       Format file size utility, it converts file size into KB, MB, GB, TB, PB units
    """
    try:
        num = long(i)
    except ValueError:
        return "N/A"
    for item in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if num < 1024.:
            return "%3.1f%s" % (num, item)
        num /= 1024.

def setsqlalchemylogger(hdlr, level):
    """Set up logging for SQLAlchemy"""
    logging.getLogger('sqlalchemy.engine').setLevel(level)
    logging.getLogger('sqlalchemy.orm.unitofwork').setLevel(level)
    logging.getLogger('sqlalchemy.pool').setLevel(level)

    logging.getLogger('sqlalchemy.engine').addHandler(hdlr)
    logging.getLogger('sqlalchemy.orm.unitofwork').addHandler(hdlr)
    logging.getLogger('sqlalchemy.pool').addHandler(hdlr)

def setcherrypylogger(hdlr, level):
    """Set up logging for CherryPy"""
    logging.getLogger('cherrypy.error').setLevel(level)
    logging.getLogger('cherrypy.access').setLevel(level)

    logging.getLogger('cherrypy.error').addHandler(hdlr)
    logging.getLogger('cherrypy.access').addHandler(hdlr)

