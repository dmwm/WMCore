"""
Implementation of Receiver sub-component.

ReceiverLogic class contains all received messages handling and runs
on background spawned from Receiver class, which threading.Thread or 
later perhaps multiprocessing.Process wrapper.

On Python 2.6.4 and pyzmq-2.1.7 having issues with shutting down the 
Receiver, it would basically hang on "recvfrom(4, ..." C call and never
resume.
   
Possible future multiprocessing.Process implementation would require
combining ReceiverLogic.start(), ._processControlMsg() together otherwise
too many class attributes would have to be turned into shared memory objects
(e.g. multiprocessing.Value). Definitely ReceiverLogic._isReady flag would
have to be shared object.

"""



import time
import logging
from threading import Thread

import zmq

from WMCore.Alerts.Alert import RegisterMsg, UnregisterMsg, ShutdownMsg



class ReceiverLogic(object):
    """
    Basic message receiver that handles alerts sent from multiple clients.
    Clients can also register/unregister via control messages which helps
    during testing.
    
    The class starts up with a target port to bind to for work and optionally
    for control messages. A handler method (callable) is used to handle alert
    data from the work receiver. It's the Processor instance. 
    
    If you want the polling loop to stop after all senders have unsubscribed,
    set closeWhenEmpty to True before calling start and it will shut down
    when the last sender unregisters.
    
    Example:
    
    def printer(alert):
          print alert

    rec = Receiver("tcp://127.0.0.1:5557", printer)
    rec.closeWhenEmpty = True 
    rec.start()
    
    """
    
    # even after Shutdown message was received, wait this timeout should
    # there be still more incoming messages and shut only when none was
    # received
    TIMEOUT_AFTER_SHUTDOWN = 0.3 # [s]
    
    # wait time for a main work method start() to finish after Shutdown
    # control message
    TIMEOUT_THREAD_FINISH = 3  # [s]
        
    
    def __init__(self, target, handler, control):
        """
        target - host:port of the work channel (where the actual alerts are sent)
        control - control channel
        handler - instance of the alert Processor
        
        """        
        context = zmq.Context()
        # receiver pulls in alerts to pass to a handler
        self._workChannel = context.socket(zmq.PULL)
        self._workChannel.bind(target)
        # control messages
        self._contChannel = context.socket(zmq.SUB)
        self._contChannel.bind(control)
        self._contChannel.setsockopt(zmq.SUBSCRIBE, "")
        # address of the control channel
        self._controlAddr = control
        # handler to be called when alert data is passed in (callable)
        self._workMsgHandler = handler
        # keep map of who registered and when
        self._registSenders = {}
        # flag to control behaviour 
        self.closeWhenEmpty = False
        # flag to shutdown this Receiver instance
        self._doShutdown = False
        # flag to check when instantiating Receiver to check readiness
        self._isReady = False
        
    
    def start(self):
        """
        Method started via Thread.
        
        Main method handling incoming messages via poller.
        If shutdown conditions are set (_doShutdown flag), then the loop
        waits only timeout time for another message should none arrive, then
        the loop will be broken.
        Examining just the content of the messages, it was observed that some
        messages may get lost if Shutdown message is sent immediately after work
        messages.
        
        """
        poller = zmq.Poller()
        poller.register(self._contChannel, zmq.POLLIN)
        poller.register(self._workChannel, zmq.POLLIN)
        self._isReady = True
        logging.info("Receiver ready to accept messages ...")
        # loop and accept messages from both channels, acting accordingly
        while True:
            timeout = None
            if self._doShutdown:
                timeout = self.TIMEOUT_AFTER_SHUTDOWN * 1000 # takes milliseconds
            socks = dict(poller.poll(timeout = timeout))
            if not socks:
                # nothing was received within the timeout
                break
            # check receiver - work channel
            if socks.get(self._workChannel) == zmq.POLLIN:
                # alert data (JSON) are sent to the handler
                alert = self._workChannel.recv_json()
                self._workMsgHandler(alert)
            # check the control channel
            if socks.get(self._contChannel) == zmq.POLLIN:
                controlData = self._contChannel.recv_json()
                self._processControlData(controlData)
        self._isReady = False
        logging.info("Receiver background loop finished. Some Alerts of "
                     "'all' type may be unflushed at the Processor and will now be lost.")
        
                
    def isReady(self):
        return self._isReady
    

    def _processControlData(self, data):
        """
        Checks the control message received in the start() method
        and acts accordingly.
        
        """
        # direct shutdown command, shutdown the receiver -> terminate start()
        if data.has_key(ShutdownMsg.key):
            self._doShutdown = True
        # new sender registers itself
        if data.has_key(RegisterMsg.key):
            senderId = data[RegisterMsg.key]
            if self._registSenders.has_key(senderId):
                logging.warn("Sender '%s' is already registered, ignored." % senderId) 
            else:
                self._registSenders[senderId] = time.time()
                logging.info("Registered %s@%s" % (senderId, self._registSenders[senderId]))
        # sender unregisters itself
        if data.has_key(UnregisterMsg.key):
            # get the label of the sender
            senderId = data[UnregisterMsg.key]
            try:
                del self._registSenders[senderId]
                logging.info("Unregistered %s@%s" % (senderId, time.time()))
            except KeyError:
                logging.warn("Sender '%s' not registered, ignored." % senderId)
            # if set, check for shutdown condition when all senders
            # have unregistered themselves
            if self.closeWhenEmpty:
                if len(self._registSenders.keys()) == 0:
                    self._doShutdown = True
                    
                    
    def shutdown(self):
        """
        Convenience method to shutdown Receiver instance - it's background
        listener process. Create sender and send Shutdown message and
        terminate the process.
        
        """
        # send itself Shutdown message to shutdown
        context = zmq.Context()
        # set up control channel
        contChann = context.socket(zmq.PUB)
        contChann.connect(self._controlAddr)        
        contChann.send_json(ShutdownMsg())
        # wait until the Receiver background process shuts
        count = 0
        while self._isReady:
            time.sleep(0.1)
            count += 1
            if count > self.TIMEOUT_THREAD_FINISH * 10: # iterating by 10ths of a second
                logging.warn("Receiver background process seems not shut yet, continue anyway ...")
                break  
                
        

class ThreadReceiver(Thread):
    """
    Wrapper thread for running Receiver instance.
    
    """
    def __init__(self, target, handler, control = "tcp://127.0.0.1:5559"):
        Thread.__init__(self)
        self._receiver = ReceiverLogic(target, handler, control)
        
    
    def run(self):
        """
        Blocking loop run on background, runs here until the shutdown
        conditions are set.
        
        """
        self._receiver.start()
        
        
    def startReceiver(self):
        """
        Can't use start() method name (Thread class inheritance).
        
        """
        self.start()        
        # wait until Receiver instance is fully started and ready
        while not self._receiver.isReady():
            logging.debug("ReceiverThread waiting for Receiver to start up ...")
            time.sleep(0.1)


    def isReady(self):
       return self._receiver.isReady()             
        
        
    def shutdown(self):
        self._receiver.shutdown()
        
        
        
Receiver = ThreadReceiver        