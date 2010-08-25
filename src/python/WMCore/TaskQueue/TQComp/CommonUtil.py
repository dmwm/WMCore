#!/usr/local/bin/python
"""
__CommonUtil__

creates logger handler for logging
 
"""

__revision__ = "$Id: CommonUtil.py,v 1.2 2009/06/01 09:57:08 delgadop Exp $"
__version__ = "$Revision: 1.2 $"
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


def buildXmlResponse(msgType, fields, taskSpec = None):
   """
   BuILDS a XML response (as a string) containing a <msg> 
   element, which includes two elements: <MsgType> and <Payload>.

   The <Payload> item holds all the elements specified in the
   fields dictionary and the spec one if specified.
   """
   response = """<?xml version="1.0" ?>\n\n<msg>\n"""
   response += "<MsgType>%s</MsgType>\n" % msgType
   response += "<Payload>\n"
   
   # Concatenate all dict fields as XML elements
   def aux(f, x): 
      return "<%s>\n  %s\n</%s>" % (x, f[x], x)
   response += reduce(lambda x,y: x+y, map(lambda x: aux(fields,x), fields), '')

   if taskSpec:
     response += taskSpec
     
   response += "\n</Payload>"
   response += "\n</msg>"

#    return response.toxml("UTF-8")
   return response



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

#    return base64.b64encode(data)



################################
## it decodes the encoded value#
################################
#def dodecode(encdata):
#    """
#    __dodecode__
#    
#    converts an ojbect to its pickle form 
#    
#    Argument:
#             obj -> any object that can be pickles
#    Return:
#          string form of pickled object
#    """
#    return base64.b64decode(encdata)




##########################
# Functions for map/reduce
#########################
def bindVals(x):
    """
    Used with reduce and commas, for a dict with keys (a, b, ..),
    returns a string ":a, :b, .."
    Useful e.g. for the VALUES part of an INSERT query (using bind variables)

    reduce(commas, map(bindVals, vars))
    """
    return ':%s' % x
    
def bindWhere(x):
    """
    Used with reduce and commas, for a dict with keys (a, b, ..), 
    returns a string "a = :a, b = :b, .."
    Useful e.g. for WHERE clauses of queries using bind variables.

    reduce(commas, map(bindWhere, vars))
    """
    return "%s = :%s" % (x, x)

def commas(x,y):
    """
    Used with reduce, for a dict with keys (a, b, ..), 
    returns a string "a, b, .."
    Useful e.g. for the ennumeration of fields in a SELECT query.

    reduce(commas, vars)
    """
    return "%s, %s" % (x, y) 

def commasLB(x,y):
    """
    Used with reduce, for a dict with keys (a, b, ..), 
    returns a string "a,\nb, .."
    Useful e.g. for the print of a list with line breaks between elements.

    reduce(commasLB, vars)
    """
    return "%s,\n%s" % (x, y) 
