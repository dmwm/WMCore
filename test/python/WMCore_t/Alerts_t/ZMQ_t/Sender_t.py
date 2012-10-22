import time
import unittest
import sys
import logging
import inspect

import zmq

from WMCore.Alerts.Alert import Alert, RegisterMsg, UnregisterMsg, ShutdownMsg
from WMCore.Alerts.ZMQ.Sender import Sender
from WMCore.Alerts.ZMQ.Receiver import Receiver



class SenderTest(unittest.TestCase):
    def setUp(self):
        self.addr = "tcp://127.0.0.1:5557"
        self.control = "tcp://127.0.0.1:5559"

        logging.basicConfig(logLevel = logging.DEBUG)

        # real Receiver instance, do test real stuff rather than with
        # mock Receiver
        self.receiver = None


    def tearDown(self):
        """
        Clean up.

        """
        if self.receiver:
            print "Receiver should be stopped now."
            print "Receiver running: (isReady()): %s" % self.receiver.isReady()
            if self.receiver.isReady():
                print "Receiver shutdown ..."
                self.receiver.shutdown()
                print "Receiver running: (isReady()): %s" % self.receiver.isReady()
        self.receiver = None


    def testSenderBasic(self):
        """
        Immediate testing register, unregister messages.
        Alert messages tested as saved in the queue.

        """
        nAlerts = 10
        # start Receiver, handler is list for alerts
        # wait for control messages to arrive and test immediately
        alertsQueue = []
        handler = lambda x: alertsQueue.append(x)
        self.receiver = Receiver(self.addr, handler, self.control)
        self.receiver.startReceiver() # non blocking call

        # instantiate sender and send ...
        s = Sender(self.addr, self.control, "Sender_t")
        # nothing is registered up to now with the Receiver
        self.assertEqual(len(self.receiver._receiver._registSenders), 0)
        s.register()
        # test that RegisterMsg arrived, consider delay
        while len(self.receiver._receiver._registSenders) == 0:
            time.sleep(0.2)
        self.assertEqual(len(self.receiver._receiver._registSenders), 1)
        # send some alerts
        for i in range(0, nAlerts):
            a = Alert(Level = i, Type = "Alert")
            s(a) # actual alert message sending
        s.unregister()
        while len(self.receiver._receiver._registSenders) == 1:
            time.sleep(0.2)
        self.assertEqual(len(self.receiver._receiver._registSenders), 0)

        # this makes sure that Receiver waits certain delay even after shutdown
        # is received if there is no more messages coming
        self.receiver.shutdown()

        self.assertEqual(nAlerts, len(alertsQueue))


    def testSenderNonBlockingWhenReceiverNotAvailable(self):
        """
        Repeatedly instantiate Sender, register, send alerts, etc
        and test that the Sender is not blocking due to undelivered
        messages since no Receiver is available.
        This test shall wait (between iterations) only delay specified
        in the Sender.

        """
        iterations = 2
        nAlerts = 3
        for i in range(iterations):
            # instantiate sender and send ...
            s = Sender(self.addr, self.control, "Sender_t")
            s.register()
            # send some alerts
            for i in range(0, nAlerts):
                a = Alert(Level = 10, Type = "Alert")
                s(a) # actual alert message sending
            s.unregister()
            # call destructor explicitly, the hanging should not occur here
            del s
        # no need to try receiving these alerts - shall be discarded with
        # calling the sender's destructor



if __name__ == "__main__":
    unittest.main()
