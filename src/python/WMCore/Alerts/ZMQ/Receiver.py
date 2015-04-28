"""
Implementation of alert messages receiver.

ReceiverLogic class contains all received messages handling and runs
on background spawned from the Receiver class (via threading.Thread).
Had issues with multiprocessing.Process implementation of the Receiver.

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
        logging.info("Instantiating %s ..." % self.__class__.__name__)
        # address of the alerts channel
        self._alertsAddr = target
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
        logging.info("Initialized %s." % self.__class__.__name__)


    def _processControlData(self, data):
        """
        Checks the control message received in the start() method
        and acts accordingly.

        """
        # direct shutdown command, shutdown the receiver -> terminate start()
        if data.has_key(ShutdownMsg.key):
            logging.warn("Received Shutdown message, setting flag ...")
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


    def _setUpChannels(self):
        """
        Instantiates sockets for message communication (channels).
        Called from the thread context.

        """
        # moreover, consider setting setsockopt on sockets (channels)
        try:
            context = zmq.Context()
            # receiver pulls in alerts to pass to a handler
            self._workChannel = context.socket(zmq.PULL)
            logging.info("Receiver - going to bind (alerts target): %s" % self._alertsAddr)
            self._workChannel.bind(self._alertsAddr)
        except Exception as ex:
            logging.error("Failed to bind (alerts target) %s, reason: %s" % (self._alertsAddr, ex))
            raise

        try:
            # control messages
            self._contChannel = context.socket(zmq.SUB)
            logging.info("Receiver - going to bind (alerts control): %s" % self._controlAddr)
            self._contChannel.bind(self._controlAddr)
            self._contChannel.setsockopt(zmq.SUBSCRIBE, "")
        except Exception as ex:
            logging.error("Failed to bind (control target) %s, reason: %s" % (self._controlAddr, ex))
            raise


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
        self._setUpChannels()
        poller = zmq.Poller()
        poller.register(self._contChannel, zmq.POLLIN)
        poller.register(self._workChannel, zmq.POLLIN)
        self._isReady = True
        logging.info("Ready to accept messages (%s) ..." % self.__class__.__name__)
        # loop and accept messages from both channels, acting accordingly
        while True:
            logging.debug("Waiting for messages ...")
            timeout = None
            if self._doShutdown:
                timeout = self.TIMEOUT_AFTER_SHUTDOWN * 1000 # takes milliseconds
            socks = dict(poller.poll(timeout = timeout))
            if not socks:
                logging.info("Nothing received in %s [ms], finishing loop." % timeout)
                # nothing was received within the timeout
                break
            # check receiver - work channel
            if socks.get(self._workChannel) == zmq.POLLIN:
                # alert data (JSON) are sent to the handler
                alert = self._workChannel.recv_json()
                logging.debug("Received Alert, processing ...")
                self._workMsgHandler(alert)
            # check the control channel
            if socks.get(self._contChannel) == zmq.POLLIN:
                controlData = self._contChannel.recv_json()
                self._processControlData(controlData)
        self._isReady = False
        logging.info("Closing ZQM sockets ...")
        poller.unregister(self._contChannel)
        poller.unregister(self._workChannel)
        self._workChannel.close()
        self._contChannel.close()
        logging.info("Receiver background loop finished. Some Alerts of "
                     "'soft' level/threshold may be unflushed at the Processor and will now be lost.")


    def isReady(self):
        return self._isReady


    def shutdown(self):
        """
        Convenience method to shutdown Receiver instance - it's background
        listener process. Create sender and send Shutdown message and
        terminate the process.

        """
        logging.info("Shutting down %s ..." % self.__class__.__name__)
        # send itself Shutdown message to shutdown
        context = zmq.Context()
        # set up control channel
        contChann = context.socket(zmq.PUB)
        contChann.connect(self._controlAddr)
        contChann.send_json(ShutdownMsg())
        contChann.close()
        # wait until the Receiver background process shuts
        count = 0
        logging.info("Waiting for %s to finish ..." % self.__class__.__name__)
        while self._isReady:
            time.sleep(0.1)
            count += 1
            if count > self.TIMEOUT_THREAD_FINISH * 10: # iterating by 10ths of a second
                logging.warn("Receiver background process seems not shut yet, continue anyway ...")
                break
        logging.info("Shutdown %s finished." % self.__class__.__name__)



class ThreadReceiver(Thread):
    """
    Wrapper thread for running Receiver instance.

    """
    def __init__(self, target, handler, control = None):
        logging.info("Instantiating %s..." % self.__class__.__name__)
        Thread.__init__(self)
        self._receiver = ReceiverLogic(target, handler, control)
        logging.info("Initialized %s." % self.__class__.__name__)


    def run(self):
        """
        Blocking loop run on background, runs here until the shutdown
        conditions are set.

        """
        logging.info("Started %s." % self.__class__.__name__)
        self._receiver.start()


    def startReceiver(self):
        """
        Can't use start() method name (Thread class inheritance).

        """
        logging.info("Starting %s ..." % self.__class__.__name__)
        self.start()
        # wait until Receiver instance is fully started and ready
        while not self._receiver.isReady():
            logging.debug("Waiting for %s to start up ..." %
                          self.__class__.__name__)
            time.sleep(0.1)


    def isReady(self):
        return self._receiver.isReady()


    def shutdown(self):
        self._receiver.shutdown()



Receiver = ThreadReceiver
