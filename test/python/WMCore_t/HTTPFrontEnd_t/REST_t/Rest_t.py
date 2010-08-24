#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#pylint: disable-msg=E1101,C0103,R0902
"""
Rest test module
"""

__revision__ = "$Id:"
__version__ = "$Revision:"
__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"

import logging
import threading
from   threading import Thread
import time
import unittest

# load TestRestServer and TestRestClient
from WMCore.HTTPFrontEnd.REST.services.test.TestRestServer import restservice
from WMCore.HTTPFrontEnd.REST.services.test.TestRestClient import restclient

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

    _setup_done = False
    _teardown = False

    def setUp(self):
        """
        setup for test.
        """
        if not RestTest._setup_done:
            logging.basicConfig(level=logging.NOTSET,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')
            myThread = threading.currentThread()
            myThread.logger = logging.getLogger('RestTest')
            RestTest._setup_done = True

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

    def runTest(self):
        """
        Run the proxy test
        """
        self.testA()
if __name__ == '__main__':
    unittest.main()

