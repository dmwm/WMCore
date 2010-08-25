#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902
"""
Rest test module
"""
__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id: Rest_t.py,v 1.5 2009/10/13 23:00:06 meloam Exp $"
__version__ = "$Revision: 1.5 $"

import logging
import threading
from   threading import Thread
import time
import unittest

# load TestRestServer and TestRestClient
from WMCore.HTTPFrontEnd.REST.services.test.TestRestServer import restservice
from WMCore.HTTPFrontEnd.REST.services.test.TestRestClient import restclient
from WMCore.HTTPFrontEnd.REST.services.test.TestFormatter import TestFormatter
from WMCore.HTTPFrontEnd.REST.services.test.TestModel import TestModel
from WMCore.HTTPFrontEnd.REST.services.rest.RestServer import RestServer

class BadModel:
    """
       Example of bad model class implementation, who miss to
       implement all required method (get, create, delete, update).
    """
    def __init__(self):
        self.datatype = 'text/plain'
    def getdata(self, method, params=None):
        """Example of getdata implementation"""
        data = "TestModel getdata method=%s params=%s" % \
               (str(method),str(params))
        datatype = self.datatype
        return data, datatype

    def createdata(self, method, params=None):
        """Example of createdata implementation"""
        data = "TestModel getdata method=%s params=%s" % \
               (str(method),str(params))
        datatype = self.datatype
        return data, datatype

class BadFormatter(object):
    """Simple formatter class. It should format input data according
       to returned MIME type
    """
    def __init__(self):
        self._data = 0 # hold some data
    def to_xml(self, data):
        """This method shows how to convert input data into XML form"""
        # you can do something with data
        self._data = data
    def to_txt(self, data):
        """This method shows how to convert input data into TXT form"""
        # you can do something with data
        self._data = data


def badrestmodel():
    """REST service based on BadModel"""
    url = 'http://localhost:8080/services/rest'
    model = BadModel()
    formatter = TestFormatter()
    verbose = 0
    rest = RestServer(model, formatter, url, verbose)
    conf = {'/':{'request.dispatch':cherrypy.dispatch.MethodDispatcher()}}
    cherrypy.quickstart(rest, '/services', config=conf)

def badrestformatter():
    """REST service based on BadModel"""
    url = 'http://localhost:8080/services/rest'
    model = TestModel()
    formatter = BadFormatter()
    verbose = 0
    rest = RestServer(model, formatter, url, verbose)
    conf = {'/':{'request.dispatch':cherrypy.dispatch.MethodDispatcher()}}
    cherrypy.quickstart(rest, '/services', config=conf)

class MyThread(Thread):
    """ My thread class which will run RestServer """
    def __init__(self):
        Thread.__init__(self)
    def run(self):
        """Run REST service"""
        restservice()

class RestTest(unittest.TestCase):
    """
    TestCase for RestServer and RestClient module 
    """
    def setUp(self):
        """
        code to execute to in preparation for the test
        """
        logging.basicConfig(level=logging.NOTSET,
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%m-%d %H:%M',
            filename='%s.log' % __file__,
            filemode='w')
        myThread = threading.currentThread()
        myThread.logger = logging.getLogger('RestTest')

    def tearDown(self):
        """code to execute to clean up after tests """
        pass
    
    def testA(self):
        """
        Mimics start-up of RestServer in one thread and
        submission of requests from RestClient.
        """
        # make a thread with RestServer
        print('--- Make a thread from RestServer\n')
        thr = MyThread()
        thr.start()
        print('--- Waiting a few seconds to make sure everything is running')
        time.sleep(3)
        print('--- Call RestClient')
        # start Rest client
        restclient()
        print('--- At this point all our requests should be completed')
        RestTest._teardown = True

    def testB(self):
        """
        Test REST server if supplied model class does not implement
        all required methods
        """
        try:
            badrestmodel()
        except AttributeError:
            print('--- Test BadModel class, we expect ERROR here')
            pass
        else:
            fail("expected a AttributeError")
        self.assertRaises(AttributeError,badrestmodel)

    def testC(self):
        """
        Test REST server if supplied formatter class does not implement
        all required methods
        """
        try:
            badrestformatter()
        except AttributeError:
            print('--- Test BadFormatter class, we expect ERROR here')
            pass
        else:
            fail("expected a AttributeError")
        self.assertRaises(AttributeError,badrestformatter)

 
if __name__ == '__main__':
    unittest.main()

