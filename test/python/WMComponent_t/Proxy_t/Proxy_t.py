#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
Proxy test TestProxy module and the harness
"""

__revision__ = "$Id: Proxy_t.py,v 1.5 2008/10/03 12:36:05 fvlingen Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "fvlingen@caltech.edu"

import commands
import cPickle
import logging
import os
import threading
import time
import unittest

from WMComponent.Proxy.Proxy import Proxy
from WMComponent.Proxy.ProxyMsgs import ProxyMsgs

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory


# local import to re-create the schema
from WMComponent_t.Proxy_t.OldMsgService import OldMsgService

class ProxyTest(unittest.TestCase):
    """
    TestCase for TestProxy module 
    """

    _setup_done = False
    _teardown = False
    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """
        if not ProxyTest._setup_done:
            msg = """
To run this test you need to have an old msg service 
setup in a different database and its contact parameters 
need to be defined in the PROXYDATABASE variable (press key to continue")
            """
            raw_input(msg)
            logging.basicConfig(level=logging.NOTSET,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            myThread = threading.currentThread()
            myThread.logger = logging.getLogger('ProxyTest')
            myThread.dialect = 'MySQL'

            options = {}
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)

            myThread.dbi = dbFactory.connect()
            myThread.transaction = Transaction(myThread.dbi)


            # need to create these tables for testing.
            factory = WMFactory("msgService", "WMCore.MsgService."+ \
                myThread.dialect)
            create = factory.loadObject("Create")
            createworked = create.execute(conn = myThread.transaction.conn)
            if createworked:
                logging.debug("MsgService tables created")
            else:
                logging.debug("MsgService tables could not be created, \
                    already exists?")
            # as the example uses threads we need to create the thread
            # tables too.
            factory = WMFactory("threadpool", "WMCore.ThreadPool."+ \
                myThread.dialect)
            create = factory.loadObject("Create")
            createworked = create.execute(conn = myThread.transaction.conn)
            if createworked:
                logging.debug("ThreadPool tables created")
            else:
                logging.debug("ThreadPool tables could not be created, \
                    already exists?")
            myThread.transaction.commit()
            # create the schema in the proxy database
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("PROXYDATABASE"), \
                options)

            dbi = dbFactory.connect()
            transaction = Transaction(dbi)
            create = OldMsgService()
            create.execute(conn = transaction.conn)
            transaction.commit()
 

            ProxyTest._setup_done = True

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()
        if ProxyTest._teardown and myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))

        ProxyTest._teardown = False


    def testA(self):
        """
        Mimics creation of component and handles come messages.
        """
        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/Proxy/DefaultConfig.py'))

        # details will be a pickled dictionary
        details = {}
        # these are default values for testing with format
        # mysql://user:pass@....
        details['contact'] = os.getenv('PROXYDATABASE')
        # subscription contains the default diagnostic messages
        # Stop, Logging.Debug,.... and some special messages such
        # as ProxySubscribe the latter is a signal to this proxy
        # to subscribe the sender of this message to its payload message
        details['subscription'] = ['Logging.DEBUG', \
        'Logging.INFO', 'Logging.ERROR', 'Logging.NOTSET', \
        'LogState', 'Stop', 'StopAndWait', 'JobSuccess', \
        'JobFailure', 'JobCreate']
        config.Proxy.PXY_Classic_1 = cPickle.dumps(details)


        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "fvlingen@caltech.edu"
        config.Agent.teamName = "Lakers"
        config.Agent.agentName = "Lebron James"

        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR")

        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql' 
        config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.CoreDatabase.user = os.getenv("DBUSER")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        config.CoreDatabase.hostname = os.getenv("DBHOST")
        config.CoreDatabase.name = os.getenv("DBNAME")

        testProxy = Proxy(config)
        # make this a thread so we can send messages.
        print('--Make a thread from the proxy component\n')
        thread = threading.Thread(target = testProxy.startComponent)
        thread.start()
        # we have our thread with component, now create 2 message services one
        # old one and one new one to test the proxy functionality.
        print('--Create a new msgService instance '+\
        '(these represent other components in our test)\n')
        myThread = threading.currentThread()
        newMsgService = myThread.factory['msgService'].loadObject("MsgService")
        myThread.transaction.begin()
        newMsgService.registerAs("newComponent")
        myThread.transaction.commit()
        print('--Create an old msgService instance\n')
        oldMsgService = ProxyMsgs(os.getenv("PROXYDATABASE"))
        oldMsgService.registerAs("oldComponent")
        print('--Waiting a few seconds to make sure everything is running')
        time.sleep(3)
        # our test proxy is subscribed to ProxySubscribe and will subscribe
        # itself to the payload. Note we subscribe twice (one local, one proxy):
        print('--Old msgService (proxy)subscribes to "ATestMessage1"\n')
        oldMsgService.subscribeTo("ATestMessage1")
        oldMsgService.publish("ProxySubscribe","ATestMessage1")
        print('--Old msgService (proxy)subscribes to "ATestMessage2"\n')
        oldMsgService.subscribeTo("ATestMessage2")
        oldMsgService.publish("ProxySubscribe","ATestMessage2")
        print('--New msgService subscribes to JobSuccess and '+\
        'JobFailure (these are proxy subscribed to by our proxy comonent)\n')
        myThread.transaction.begin()
        newMsgService.subscribeTo("JobSuccess")
        newMsgService.subscribeTo("JobFailure")
        myThread.transaction.commit()
        print('--Waiting a few seconds to make sure '+\
        'the (proxy) subscriptions arrived\n')
        time.sleep(10)
        # now we can publish a message in the new component that will be 
        # forwarded by the proxy
        myThread.transaction.begin()
        msg = {'name':'ATestMessage1', 'payload':'forOldPA'}
        print('--Publish 10 messages in new msgService : '+str(msg)+'\n')
        for i in xrange(0, 10):
            newMsgService.publish(msg)
        myThread.transaction.commit()
        newMsgService.finish()
        # wait at the old service for the message to arrive.
        print('--Waiting in oldMsgService for 10 messages\n')
        for i in xrange(0, 10):
            type, payload = oldMsgService.get()
            assert type == 'ATestMessage1'
            assert payload == 'forOldPA'
        print('--Old msgService sends 10 JobSuccess messages')
        for i in xrange(0, 10):
            oldMsgService.publish("JobSuccess","")
        print('--New msgService waits for it')
        for i in xrange(0, 10):
            msg = newMsgService.get()
            assert msg['name'] == 'JobSuccess'
        print('--New component is (proxy)subscribed to Stop message\n')
        print('--Old message service sends Stop message\n')
        # as we are running the component in a thread we want the component to 
        # stop once all its threads minus itself and the main thread are done
        oldMsgService.publish("StopAndWait","2")
        print('--Component in thread will receive this message and stop')
        while threading.activeCount() > 1:
            time.sleep(3)
            print(str(threading.activeCount())+' threads active')
        ProxyTest._teardown = True

    def runTest(self):
        """
        Run the proxy test
        """
        self.testA()
if __name__ == '__main__':
    unittest.main()

