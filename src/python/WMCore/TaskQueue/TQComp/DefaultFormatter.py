#!/usr/bin/env python

"""
_DefaultFormatter_

This module contains a class that is example of formatter for responses
of the RemoteMsg service (upon reception of remote messages). Any formatter
implementing the interface that this class shows may be set at configuration
time. Otherwise, the default one in this module is used.
"""

__revision__ = "$Id: DefaultFormatter.py,v 1.3 2009/06/07 23:14:27 valya Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "antonio.delgado.peris@cern.ch"


from cherrypy import response
from cherrypy.lib.cptools import accept
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json

class DefaultFormatter(object):
    """ 
    _DefaultFormatter_. This formatter is capable of encoding in json, using
    json. If you need support for other data types, please implement
    your own class offering a 'supportedTypes' attribute and a 'format' method
    (include the cherrypy methods in this example for user preference).
    """
    def __init__(self):
        self.supportedTypes = ['text/json', 'text/x-json', 'application/json']
        

    def format(self, data):
        """
        Returns a string represent 'data' in one of the supported types.
        If the client has expressed preference, this should be honored if
        possible. If not possible (or there was no preference) a default 
        type will be used.
        """
        datatype = accept(self.supportedTypes)
        response.headers['Content-Type'] = datatype
    
        if datatype in ('text/json', 'text/x-json', 'application/json'):
            # Serialise to json
            return self.tojson(data)
    
        # Default... return in json anyway
        return self.tojson(data)


    def tojson(self, data):
        """
        Returns a string represent 'data' in json format.
        """
        return json.dumps(data)
