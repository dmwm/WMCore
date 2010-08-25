#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
Proxy test TestProxy module and the harness.

This test verifies how the proxy module works. Below some text on how you can
use it to actually integrate it with components using the old message service.

Setup: the minimum setup is: 

-one component using the old message service (lets call this OC)
-an agent (which is a collection of components) based on the old 
message service (lets call this OA)
-database containing the old schema (lets call this OS)
-proxy component based on the new WMCore libs (lets call this PC
-database containing the new schema (lets call this NS)
-component based on the new WMCore lib (lets call this NC)
-an agent (which is a collection of components) based on the new
message service and wmcore libs (lets call this NA)

You want to either do one (or both) of the following:

OC produces messages that the NC needs to receive
NC produces messages that the OC needs to receive

Note: in order to deploy the wmcore database schema you can 
use the WMCORE/bin/wmcore-db-init (check options to augment this schema).

Before you start your proxy component you need to make sure that it starts
with the right configuration file (see for example: 
WMCORE/src/python/WMComponent/Proxy/DefaultConfig.py

Note: in order to generate a config file for a WMCore based agent you can use
WMCORE/bin/wmcore-new-config script (check options for this script).

Note: to start your new component(s) as daemons you can use the
WMCORE/bin/wmcoreD script (check options for this script).

In the config file proxy details (there is 1 set of details per agent to which 
an OC belongs, you define: the database contact string ( the string needed by the proxy
component to access the old message service) and a list of subscriptions (a set of messages
that if they are published in the OA containing the OC and need to be forwarded to the proxy which 
will publish them in the NA (consisting of NCs) that is based on the NS.

Most of the time there is only one OA and one NA so the config file will contain
one set of details. Below an example of a proxy details that you find in the config file:

details['contact'] = os.getenv('PROXYDATABASE')
# subscription contains the default diagnostic messages
# Stop, Logging.Debug,.... and some special messages such
# as ProxySubscribe the latter is a signal to this proxy
# to subscribe the sender of this message to its payload message
details['subscription'] = ['Logging.DEBUG','Logging.INFO','Logging.ERROR',\
   'Logging.NOTSET','LogState','Stop','JobSuccess','JobFailure','JobCreate']
config.Proxy.PXY_Classic_1 = cPickle.dumps(details)

Note that the details are pickled  and the attribute it is assigned to is 
prefix with PXY_

The Proxy component will locate these attributes and assume it contains proxy
details.

The Proxy component has (per remote component) a so called proxy queue. This queue
has an input buffer and output buffer. The output buffer is for messages that are 
going to the other OA, the input buffer is for messages coming from the OA and need 
to be dispatched to the NC in the NA.

When it locates the subscription messages in the config file it will send a message to the
output buffer of the form: name="ProxySubscribe" payload=<subscription message> . 
ProxySubscribe is a 'meta message and the output buffer knows that this means it has to 
subscribe to messages of type <subscription message> in the OA. 

It then also does a special subscription of the form: name="ProxySubscribe" 
payload="ProxySubscribe" . This means that components in the OA can publish messages
of type "ProxySubscribe" and with the value (or payload) the message type that they want
to receive in the OA whenever a NC in the NA publishes this message type.

To summarize: 

-message(types) a NC in the NA wants to receive from OC in OA are defined
in the configuration file and the proxy component makes sure it subscribes for them 
in the old message service.

-message(types) a OC in the OA wants to receive from a NC in the NA need to 
be published through the "ProxySubscribe" message as this is the message that the proxy
component (which lives in the NA) picks up.

-Whether you want N OCs in an OC to communicate with M NC in an NA you only need one proxy component
but you do need to define which of the messages need to be passed through the proxy component.

-In order for the proxy component to communicate with the old message service it uses the 
WMComponent.Proxy.ProxyMsgs module. This is a wrapper for queries and is compliant wit the old 
message service tables.


Note: this proxy component is specific for communication between OC and NC and the proxy component
directly interfaces with the OS. If we want to generalize this we would need to have a generic ProxyMsgs
module and proxies in both components where the proxies communicate with eachother through the ProxyMsgs
module and via for example http requests.

An example:

Lets say a NC publishes 'messageX' to which NCs and OCs need to subscribe to:

-For NCs this is no problem as they subscribe to it via the well establised 
mechanism
-OCs need to do 2 things: 
(1) subscribe themselves with within the OA to these 'messageX' types via 
the well established mechanism through the old message service
(2) At least one OC (or perhaps some other app) needs to publish a message 
of type 'ProxySubscribe' with payload 'messageX' . This is picked up by the 
Proxy component and allows it to relay messages of type messageX to the OCs

Lets say a OC publishes 'messageY' to which NCs and OCs need to subscribe to:

-For OCs this is no problem as they subscribe to it via the well established 
mechanism through the old message service
-NCs need to do 2 things:
(1) Subscribe themselves within the NA to this 'messageY' types via the
well established mechanism in the NA through the new message service.
(2) the message needs to be defined in the subscription field in for the 
proxy component so the proxy component will subscribe to it at the OA.

So the additional 'work' so to say w.r.t. to one message service is that per 
message that needs to be published both ways and is published on both sides 
(e.g. messageX = messageY) is and additional 'ProxySubscribe' publication 
in the OA and a specification of the message type in the proxy config file. 
The subscription part of the individual components would have need to be 
done anyway with one message service so that part is not additional work.

Basically the additional work is telling the other msg service: send this
particular message to the proxy which forwards it.

"""





import commands
import cPickle
import logging
import os
import threading
import time
import unittest
import WMCore.WMInit
from WMComponent.Proxy.Proxy import Proxy
from WMComponent.Proxy.ProxyMsgs import ProxyMsgs

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit


# local import to re-create the schema
from WMComponent_t.Proxy_t.OldMsgService import OldMsgService

class ProxyTest(unittest.TestCase):
    """
    TestCase for TestProxy module 
    """

    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema()

        return
        
        #myThread = threading.currentThread()
        #myThread.transaction.begin()
        #create = OldMsgService()
        #create.execute(conn = myThread.transaction.conn)
        #myThread.transaction.commit()
        #return

    def tearDown(self):
        """
        Database deletion
        """
        self.testInit.clearDatabase()


    def testA(self):
        """
        Mimics creation of component and handles come messages.
        """
        # read the default config first.
        config = self.testInit.getConfiguration(os.path.join(WMCore.WMInit.getWMBASE(), \
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

        # initialize component with proper configuration.
        testProxy = Proxy(config)
        # make this a thread so we can send messages.
        logging.debug('--Make a thread from the proxy component\n')
        thread = threading.Thread(target = testProxy.startComponent)
        thread.start()
        # we have our thread with component, now create 2 message services one
        # old one and one new one to test the proxy functionality.
        # if you just want to test a new component just instantiate the new
        # the new message service.
        logging.debug('--Create a new msgService instance '+\
        '(these represent other components in our test)\n')
        myThread = threading.currentThread()
        newMsgService = myThread.factory['msgService'].loadObject("MsgService")
        myThread.transaction.begin()
        newMsgService.registerAs("newComponent")
        myThread.transaction.commit()
        logging.debug('--Create an old msgService instance\n')
        oldMsgService = ProxyMsgs(os.getenv("PROXYDATABASE"))
        oldMsgService.registerAs("oldComponent")
        logging.debug('--Waiting a few seconds to make sure everything is running')
        time.sleep(3)
        # our test proxy is subscribed to ProxySubscribe and will subscribe
        # itself to the payload. Note we subscribe twice (one local, one proxy):
        logging.debug('--Old msgService (proxy)subscribes to "ATestMessage1"\n')
        oldMsgService.subscribeTo("ATestMessage1")
        oldMsgService.publish("ProxySubscribe","ATestMessage1")
        logging.debug('--Old msgService (proxy)subscribes to "ATestMessage2"\n')
        oldMsgService.subscribeTo("ATestMessage2")
        oldMsgService.publish("ProxySubscribe","ATestMessage2")
        logging.debug('--New msgService subscribes to JobSuccess and '+\
        'JobFailure (these are proxy subscribed to by our proxy comonent)\n')
        myThread.transaction.begin()
        newMsgService.subscribeTo("JobSuccess")
        newMsgService.subscribeTo("JobFailure")
        myThread.transaction.commit()
        logging.debug('--Waiting a few seconds to make sure '+\
        'the (proxy) subscriptions arrived\n')
        time.sleep(10)
        # now we can publish a message in the new component that will be 
        # forwarded by the proxy to the old message service.
        myThread.transaction.begin()
        msg = {'name':'ATestMessage1', 'payload':'forOldPA'}
        logging.debug('--Publish 10 messages in new msgService : '+str(msg)+'\n')
        for i in xrange(0, 10):
            newMsgService.publish(msg)
        myThread.transaction.commit()
        newMsgService.finish()
        # wait at the old service for the message to arrive.
        logging.debug('--Waiting in oldMsgService for 10 messages\n')
        for i in xrange(0, 10):
            type, payload = oldMsgService.get()
            self.assertEqual( type ,  'ATestMessage1' )
            self.assertEqual( payload ,  'forOldPA' )
        logging.debug('--Old msgService sends 10 JobSuccess messages')
        for i in xrange(0, 10):
            oldMsgService.publish("JobSuccess","JobSuccess")
        logging.debug('--New msgService waits for it')
        for i in xrange(0, 10):
            msg = newMsgService.get()
            self.assertEqual( msg['name'] ,  'JobSuccess' )
        logging.debug('--New component is (proxy)subscribed to Stop message\n')
        logging.debug('--Old message service sends Stop message\n')
        # as we are running the component in a thread we want the component to 
        # stop once all its threads minus itself and the main thread are done
        oldMsgService.publish("StopAndWait","2")
        logging.debug('--Component in thread will receive this message and stop')
        # wait until the thread count is 1 (the main trhead) before
        # ending the test.
        while threading.activeCount() > 1:
            time.sleep(3)
            logging.debug(str(threading.activeCount())+' threads active')


if __name__ == '__main__':
    unittest.main()

