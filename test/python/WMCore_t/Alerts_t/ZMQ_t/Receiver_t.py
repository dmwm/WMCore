import time
import sys
import unittest
from multiprocessing import Process, Queue

import zmq

from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.Alert import RegisterMsg, UnregisterMsg, ShutdownMsg
from WMCore.Alerts.ZMQ.Receiver import Receiver
    
    

def worker(addr, ctrl, outputQ, nAlerts):
    """
    Start a sender and send some alert messages to
    the Receiver.
    
    """
    context = zmq.Context()
    # set up a channel to send work
    sender = context.socket(zmq.PUSH)
    sender.connect(addr)
    
    controller = context.socket(zmq.PUB)
    controller.connect(ctrl)
    
    controller.send_json(RegisterMsg("Receiver_t"))
    for i in range(0, nAlerts):
        a = Alert(Type = "Alert", Level = i)
        sender.send_json(a)
    controller.send_json(UnregisterMsg("Receiver_t"))
    
    controller.send_json(ShutdownMsg())
    
    
    
class ReceiverTest(unittest.TestCase):
    """
    Test Case for Receiver.
     
    """
    def setUp(self):
        """ 
        Start bg process that sends messages.
        
        """
        self.addr = "tcp://127.0.0.1:5557"
        self.ctrl = "tcp://127.0.0.1:5559"
        # simple printer Alert messages handler
        self.printer = lambda x : sys.stdout.write("printer handler: '%s'\n" % str(x))
        
        
    def tearDown(self):
        pass
    
    
    def _getSenderChannels(self):
        context = zmq.Context()
        # set up a channel to send work
        workChann = context.socket(zmq.PUSH)
        workChann.connect(self.addr)
        # set up control channel
        contChann = context.socket(zmq.PUB)
        contChann.connect(self.ctrl)
        return workChann, contChann        
        
            
    def testReceiverShutdownByMessage(self):
        # start a Receiver
        rec = Receiver(self.addr, self.printer, self.ctrl)
        rec.startReceiver() # non blocking call
        
        workChann, contChann = self._getSenderChannels()
        
        # send some messages to the receiver and shut it eventually
        contChann.send_json(RegisterMsg("Receiver_t"))
        workChann.send_json(Alert(Type = "Alert", Level = 10))
        contChann.send_json(UnregisterMsg("Receiver_t"))
        # terminate the Receiver
        contChann.send_json(ShutdownMsg())
                
        # wait until the Receiver is properly shut
        # this will not be necessary when shutting down by a call
        while rec.isReady():
            time.sleep(0.1)
                
        
    def testReceiverShutdownByCall(self):
        # start a Receiver
        rec = Receiver(self.addr, self.printer, self.ctrl)
        rec.startReceiver() # non blocking call
        
        workChann, contChann = self._getSenderChannels()
        
        # send some messages to the receiver and shut it eventually        
        contChann.send_json(RegisterMsg("Receiver_t"))
        workChann.send_json(Alert(Type = "Alert", Level = 20))
        contChann.send_json(UnregisterMsg("Receiver_t"))
        
        # now messages are sent so shutdown the Receiver by a convenience
        # call, should block until the Receiver finishes, don't have to wait
        rec.shutdown()
                

    def testReceiverBuferring(self):
        """
        Test sending alerts that are buffered into a queue.
        
        """
        numAlertMsgs = 7
        q = Queue()
        # start a process that generates 5 alerts, worker can run
        # as a different process, though is not necessary here
        self.p = Process(target = worker, args = (self.addr, self.ctrl, q, numAlertMsgs))
        self.p.start()
        
        # alerts received will be added to the queue
        handler = lambda x: q.put(x)
        rec = Receiver(self.addr, handler, self.ctrl)
        rec.startReceiver() # non blocking call
        
        # wait here until the Receiver is not shut
        while rec.isReady():
            time.sleep(0.2)
        
        # worker will send shutdown message and execution will resume here
        # check the content of the queue - 5 alert messages
        alertCount = 0
        while not q.empty():
            alert = q.get()
            self.failUnless(alert.has_key("Type"))
            self.assertEqual(alert["Type"], "Alert")
            alertCount += 1
        self.assertEqual(alertCount, numAlertMsgs)
    
        
    
if __name__ == "__main__":
    unittest.main()