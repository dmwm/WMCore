#!/usr/local/bin/python
"""
__CommonUtil__
"""

__revision__ = "$Id: CommonUtil.py,v 1.1 2009/04/01 12:56:01 delgadop Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "antonio.delgado.peris@cern.ch"

import logging
#pickle, encoding n decoding data
import cPickle
import simplejson
from urllib import urlencode
#import base64


###################################
# it handles the name for logging #
###################################
def getlogger(name):
    """
    __startlogging__
    this will add logging handle for incoming name
    
    Argument:
             name -> name of logging
             
    Return:
           nothing
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    #create console handler and set level to debug
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    #create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - " \
                                   "%(message)s")
    #add formatter to ch
    handler.setFormatter(formatter)
    #add ch to logger
    logger.addHandler(handler)
    return logger

###########################################
# it converts object into its json form #
###########################################
def dojson(obj):
    """
    __dojson__
    
    converts an object to its json form 
    
    Argument: any object that can be jsoned
    Return:   json string representing obj
    """
    jsonobj = simplejson.dumps( obj )
    return jsonobj

###########################################
# it converts json string to its object#
###########################################
def undojson(jsonobj):
    """
    __undojson__
    
    converts a json string to an object
    
    Argument: json string
    Return:  object
    """
    orgobj = simplejson.loads(jsonobj)
    return orgobj

###########################################
# it converts object into its pickle form #
###########################################
def dopickle(obj):
    """
    __dopickle__
    
    converts an ojbect to its pickle form 
    
    Argument:
             obj -> any object that can be pickles
    Return:
          string form of pickled object
    """
    pickleobj = cPickle.dumps( obj )
    #return pickledObj
    return pickleobj
    
###########################################
# it converts pickled string to its object#
###########################################
def undopickle(pickleobj):
    """
    __dopickle__
    
    converts an ojbect to its pickle form 
    
    Argument:
             obj -> any object that can be pickles
    Return:
          string form of pickled object
    """
    orgobj = cPickle.loads(pickleobj)
    return orgobj

###########################################
# it converts pickled string to its object#
###########################################
def doencode(data):
    """
    __doencode__
    
    converts an ojbect to an appropriate form for HTTP arguments (GET or POST)
    
    Argument:
             obj -> any object that can be pickles
    Return:
          string form of pickled object
    """
    return urlencode(data)
