"""
A few helper variables, functions and classes for testing the
Alert framework poller classes.

"""

import multiprocessing
import random
import time

import psutil

from WMCore.Alerts.ZMQ.Receiver import Receiver
from WMComponent.AlertGenerator.Pollers.Base import ProcessDetail
from WMComponent.AlertGenerator.Pollers.Base import Measurements



# template for agent's component Daemon.xml description file
daemonXmlContent = \
"""
<Daemon>
    <ProcessID Value="%(PID_TO_PUT)s"/>
    <!-- below stuff here doesn't matter, only ProcessID -->
    <ParentProcessID Value="12672"/>
    <ProcessGroupID Value="12672"/>
    <UserID Value="1000"/>
    <EffectiveUserID Value="1000"/>
    <GroupID Value="1000"/>
    <EffectiveGroupID Value="1000"/>
</Daemon>

"""


def terminateProcesses(processList):
    """
    Kills the process inc. subprocesses.
    As side effect, nulls the input processList argument.
    
    """
    for p in processList:
        proc = psutil.Process(p.pid)
        for c in proc.get_children():
            c.kill()
        proc.kill()
    processList = []
            
    
    
def subWorker():
    """
    Processes started as a subprocess from worker.
    Just to take a lot of CPU.
    
    """
    while True:
        1 + 1        
        
        

def worker(numChildren = 0, lazy = False):
    """
    Process function started via multiprocessing.Process.
    Can be lazy, can start n subprocesses.
    
    """
    if numChildren > 0:
        for i in range(numChildren):
            p = multiprocessing.Process(target = subWorker, args = ())
            p.start()
    while True:
        if lazy:
            time.sleep(0.5)
        1 + 1        
    

        
def getProcess(numChildren = 0):
    """
    Return Process instance.
    
    """
    p = multiprocessing.Process(target = worker, args = (numChildren, ))
    p.start()
    return p



def setUpReceiver(address, controlAddr):
    """
    Return set up handler, receiver pair.
    Receiver starts two channels on the address and controlAddr addresses.
    
    """
    handler = ReceiverHandler()
    receiver = Receiver(address, handler, controlAddr)
    receiver.startReceiver() # non blocking call        
    return handler, receiver



def doProcessPolling(ppti):
    """
    ppti - Process Polling Test Input instance (all variables on input to this test)
    The function is easier to reuse from here that from other test class.
    
    """    
    try:
        poller = ppti.pollerClass(ppti.config, ppti.testCase.generator)
    except Exception, ex:
        ppti.testCase.fail("%s: exception: %s" % (ppti.testCase.testName, ex))
    
    handler, receiver = setUpReceiver(ppti.testCase.generator.config.Alert.address,
                                      ppti.testCase.generator.config.Alert.controlAddr)
    
    procWorker = multiprocessing.Process(target = worker, args = ())
    procWorker.start()
    ppti.testCase.testProcesses.append(procWorker)
    
    numMeasurements = ppti.config.period / ppti.config.pollInterval
        
    # inject own input sample data provider
    # there is in fact input argument in this case which needs be ignored
    poller.sample = lambda proc_: random.randint(ppti.thresholdToTest,
                                                 ppti.thresholdToTest + ppti.thresholdDiff)

    # the process to run upon is fooled as well here
    poller._dbProcessDetail = ProcessDetail(procWorker.pid, "TestProcess")
    poller._measurements = Measurements(numMeasurements)
    
    proc = multiprocessing.Process(target = poller.poll, args = ())
    proc.start()
    ppti.testCase.assertTrue(proc.is_alive())

    if ppti.expected != 0:
        # beware - if the alert is not correctly generated, the test
        # will hang here and will be waiting for it
        while len(handler.queue) == 0:
            time.sleep(ppti.config.pollInterval / 5)
    else:
        time.sleep(ppti.config.period * 2)
        
    procWorker.terminate()        
    proc.terminate()
    poller.shutdown()
    receiver.shutdown()
    ppti.testCase.assertFalse(proc.is_alive())
    
    if ppti.expected != 0:
        # there should be just one alert received, poller should have the
        # change to send a second
        ppti.testCase.assertEqual(len(handler.queue), ppti.expected)
        a = handler.queue[0]
        # soft threshold - alert should have soft level
        ppti.testCase.assertEqual(a["Level"], ppti.level)
        ppti.testCase.assertEqual(a["Component"], ppti.testCase.generator.__class__.__name__)
        ppti.testCase.assertEqual(a["Source"], poller.__class__.__name__)            
    else:
        ppti.testCase.assertEqual(len(handler.queue), 0)



def doGenericValueBasedPolling(ti):
    """
    ti - Test Input instance (all variables on input to this test)
    The function is easier to reuse from here that from other test class.
    Used for directory size polling.
    Generic value based polling (e.g. CouchDB HTTP status codes polling).
    
    """    
    try:
        poller = ti.pollerClass(ti.config, ti.testCase.generator)
    except Exception, ex:
        ti.testCase.fail("%s: exception: %s" % (ti.testCase.testName, ex))        
    # inject own input sample data provider, don't care about the directory
    poller.sample = lambda dir: random.randint(ti.thresholdToTest,
                                               ti.thresholdToTest + ti.thresholdDiff)
    
    handler, receiver = setUpReceiver(ti.testCase.generator.config.Alert.address,
                                      ti.testCase.generator.config.Alert.controlAddr)    
    proc = multiprocessing.Process(target = poller.poll, args = ())
    proc.start()
    ti.testCase.assertTrue(proc.is_alive())

    # wait to poller to work now ... wait for alert to arrive
    if ti.expected != 0:
        while len(handler.queue) == 0:
            time.sleep(ti.config.pollInterval / 10)
    else:
        time.sleep(ti.config.pollInterval * 2)
        
    proc.terminate()
    poller.shutdown()
    receiver.shutdown()
    ti.testCase.assertFalse(proc.is_alive())

    if ti.expected != 0:   
        # there should be just one alert received, poller should have the
        # change to send a second
        ti.testCase.assertEqual(len(handler.queue), ti.expected)
        a = handler.queue[0]
        # soft threshold - alert should have soft level
        ti.testCase.assertEqual(a["Level"], ti.level)
        ti.testCase.assertEqual(a["Component"], ti.testCase.generator.__class__.__name__)
        ti.testCase.assertEqual(a["Source"], poller.__class__.__name__)
        d = a["Details"]
        ti.testCase.assertEqual(d["threshold"], ti.thresholdToTest)
    else:
        ti.testCase.assertEqual(len(handler.queue), 0)
        

        
class TestInput(object):
    """
    Helper class used for passing necessary configuration attributes into the
    helper test runners which are common for MySQL, CouchDb, etc.
    
    """
    def __init__(self):
        # actual poller class to instantiate and test
        self.pollerClass = None
        # configuration for the poller class
        self.config = None
        # threshold which the poller class shall test in this test call
        self.thresholdToTest = None
        # level of alert corresponding to this threshold testing
        self.level = None
        # how many alerts are expected to be received
        self.expected = None
        # feeding random numbers to the poller, what should be the deviation
        self.thresholdDiff = None
        # reference to the calling testcase instance - because of assertions ...
        self.testCase = None 

        

class AlertGeneratorMock(object):
    """
    This class simulates config instance forwarding as real AlertGenerator
    does and that is the only purpose.
    
    """
    def __init__(self, config):
        self.config = config
        
        

class SenderMock(object):
    """
    Emulates Alerts Sender class. It has to provide callable and
    in this case it just catches outgoing Alert messages for final
    test inspection.
    
    """
    def __init__(self):
        self.queue = []
       
        
    def __call__(self, alert):
        self.queue.append(alert)
        
        

class ReceiverHandler(object):
    """
    Handler class for Alert Receiver.
    Incoming alerts are stored into a list.
    
    Current implementation of Receiver is that it operates on background
    as a Thread - thus can use just plain list here.
    If multiprocessing was involved, the Alerts storage would have to be
    multiprocessing.Queue.
    
    """
    def __init__(self):
        self.queue = []
        
        
    def __call__(self, alert):
        self.queue.append(alert)