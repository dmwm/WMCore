"""
A few helper variables, functions and classes for testing the
Alert framework poller classes.

"""

import os
import sys
import random
import time
import logging
import datetime
import signal

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



def setUpReceiver(address, controlAddr):
    """
    Return set up handler, receiver pair.
    Receiver starts two channels on the address and controlAddr addresses.

    """
    handler = ReceiverHandler()
    receiver = Receiver(address, handler, controlAddr)
    receiver.startReceiver() # non blocking call
    return handler, receiver



def doGenericPeriodAndProcessPolling(ti):
    """
    ti - Test Input instance (all variables on input to this test)
    The function is easier to reuse from here that from other test class.
    This helper function is also used for generic period polling.

    """
    try:
        poller = ti.pollerClass(ti.config, ti.testCase.generator)
    except Exception, ex:
        ti.testCase.fail("%s: exception: %s" % (ti.testCase.testName, ex))

    handler, receiver = setUpReceiver(ti.testCase.generator.config.Alert.address,
                                      ti.testCase.generator.config.Alert.controlAddr)

    pid = os.getpid()

    numMeasurements = ti.config.period / ti.config.pollInterval

    # inject own input sample data provider
    # there is in fact input argument in this case which needs be ignored
    poller.sample = lambda proc_: random.randint(ti.thresholdToTest,
                                                 ti.thresholdToTest + ti.thresholdDiff)

    # the process to run upon is fooled as well here
    poller._dbProcessDetail = ProcessDetail(pid, "TestProcess")
    poller._measurements = Measurements(numMeasurements)
    poller.start()
    ti.testCase.assertTrue(poller.is_alive())

    if ti.expected != 0:
        # beware - if the alert is not correctly generated, the test
        # will hang here and will be waiting for it
        # #2238 AlertGenerator test can take 1 hour+ (and fail)
        # fail 2mins anyway if alert is not received
        timeLimitExceeded = False
        startTime = datetime.datetime.now()
        limitTime = 2 * 60 # seconds
        while len(handler.queue) == 0:
            time.sleep(ti.config.pollInterval / 5)
            if (datetime.datetime.now() - startTime).seconds > limitTime:
                timeLimitExceeded = True
                break
    else:
        time.sleep(ti.config.period * 2)

    poller.terminate()
    receiver.shutdown()
    ti.testCase.assertFalse(poller.is_alive())

    if ti.expected != 0:
        # #2238 AlertGenerator test can take 1 hour+ (and fail)
        # temporary measure from above loop:
        if timeLimitExceeded:
            ti.testCase.fail("No alert received in %s seconds." % limitTime)
        # there should be just one alert received, poller should have the
        # change to send a second
        ti.testCase.assertEqual(len(handler.queue), ti.expected)
        a = handler.queue[0]
        # soft threshold - alert should have soft level
        ti.testCase.assertEqual(a["Level"], ti.level)
        ti.testCase.assertEqual(a["Component"], ti.testCase.generator.__class__.__name__)
        ti.testCase.assertEqual(a["Source"], poller.__class__.__name__)
    else:
        ti.testCase.assertEqual(len(handler.queue), 0)


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
    poller.start()
    ti.testCase.assertTrue(poller.is_alive())

    # wait to poller to work now ... wait for alert to arrive
    if ti.expected != 0:
        # #2238 AlertGenerator test can take 1 hour+ (and fail)
        # fail 2mins anyway if alert is not received
        timeLimitExceeded = False
        startTime = datetime.datetime.now()
        limitTime = 2 * 60 # seconds
        while len(handler.queue) == 0:
            time.sleep(ti.config.pollInterval / 10)
            if (datetime.datetime.now() - startTime).seconds > limitTime:
                timeLimitExceeded = True
                break
    else:
        time.sleep(ti.config.pollInterval * 2)

    poller.terminate()
    receiver.shutdown()
    ti.testCase.assertFalse(poller.is_alive())

    if ti.expected != 0:
        # #2238 AlertGenerator test can take 1 hour+ (and fail)
        # temporary measure from above loop:
        if timeLimitExceeded:
            ti.testCase.fail("No alert received in %s seconds." % limitTime)
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

    """
    def __init__(self):
        self.queue = []


    def __call__(self, alert):
        self.queue.append(alert)
