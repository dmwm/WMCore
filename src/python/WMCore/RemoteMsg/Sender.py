#!/usr/bin/env python

"""
_Sender_

This module contains a class whose instances can be used to publish messages
to other RemoteMsg instances.
"""





#import os
#import inspect

import logging
import urllib2 
#from urllib import urlencode
#from CommonUtil import dopickle, doencode
from CommonUtil import dojson, doencode

class Sender(object):
    """ 
    _Sender_
    
    """
    def __init__(self, msgQueue, parameters):
        self.msgQueue = msgQueue

        self.port = 8030
        if 'port' in parameters:
            self.port = parameters['port']
        self.service = "msg"
        if 'service' in parameters:
            self.service = parameters['service']
        self.user = None
        if 'realm' in parameters:
            self.realm = parameters['realm']
        if 'user' in parameters:
            self.user = parameters['user']
        self.pwd = None
        if 'pwd' in parameters:
            self.pwd = parameters['pwd']
          
        self.addr = None
        self.setAddress(parameters['addresses'])

        self.mylogger = logging.getLogger("RemoteMsg")

    def setAddress(self, addresses):
        """
        Sets the list of addresses where to send messages (by 'send' method).
        """
        self.addr = addresses
        for i in xrange(len(self.addr)):
            if not ':' in self.addr[i]: 
                self.addr[i] += ':' + self.port 
            self.addr[i] += '/' + self.service


    def send(self, msgType, payload, sync = False):
        """
        Causes the object to send a message of type 'msgType' and content
        'payload' to the recipients indicated previously (setAddress).
        """
        payload = dojson(payload)
        if sync: 
            sync = 'True'
        args = doencode( [ ('msgType', msgType), ('payload', payload), \
                           ('sync',sync) ] )

        for addr in self.addr:
            msg = 'Trying to open: http://%s with %s' % (addr, args)
            self.mylogger.debug(msg)
            uri = 'http://' + addr
            try:
                # If user is not None, we use authentication
                if self.user:
                    # set up authentication info
                    authinfo = urllib2.HTTPDigestAuthHandler()
# Following would be for basic authentication (not digest)
# But does not seem to work...
#                authinfo = urllib2.HTTPBasicAuthHandler()
                    authinfo.add_password(self.realm, uri, self.user, self.pwd)
                    # build a new opener that adds authentication
                    opener = urllib2.build_opener(authinfo)
                    # install it
                    urllib2.install_opener(opener)

                    f = urllib2.urlopen(uri, args)
                else:
                    f = urllib2.urlopen(uri, args)

                # Read reply
                out = f.read()
                f.close()
             
                return out

            except Exception, inst:
                # TODO: catch auth exception and show proper message
                raise Exception("Error when trying to connect to %s: %s %s" % \
                    (addr, str(inst.__class__), str(inst)))

