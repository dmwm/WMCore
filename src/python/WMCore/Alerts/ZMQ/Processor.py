#!/usr/bin/env python
# encoding: utf-8

"""
Alert Processor pipeline.
Provides a coroutine based handling system for alerts that can be used
by a Receiver to process Alert streams sent from Agent components.

Created by Dave Evans on 2011-03-02.
Copyright (c) 2011 Fermilab. All rights reserved.

"""


from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sinks.CouchSink import CouchSink
from WMCore.Alerts.ZMQ.Sinks.EmailSink import EmailSink
from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink
from WMCore.Alerts.ZMQ.Sinks.ForwardSink import ForwardSink
from WMCore.Alerts.ZMQ.Sinks.RSSFeedSink import RSSFeedSink


sinksMap = {
    "file": FileSink,
    "couch": CouchSink,
    "email": EmailSink,
    "forward": ForwardSink,
    "rss": RSSFeedSink
}



def coroutine(func):
    """
    Decorator method used to prime coroutines.

    """
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.next()
        return cr
    return start



@coroutine
def dispatcher(targets, config):
    """
    Function to dispatch an alert to a set of targets based on the level
    of the Alert instance. 
    
    Targets arg should be a dict of coroutines to handle the appropriate
    level of alert message and contain routines for handling
    "all" and "critical" alerts.
    
    """
    # these levels shall IMO be always defined in the very input configuration
    # otherwise the altered behaviour is difficult to unmask, i.e.
    # rather than using getattr with default value on next two lines
    criticalThreshold = config.critical.level
    allThreshold = config.all.level
    while True:
        alert = (yield)
        if alert.level > allThreshold:
            targets["all"].send(alert)
        if alert.level > criticalThreshold:
            targets["critical"].send(alert)
            

            
@coroutine
def handleAll(targets, config):
    """
    Handler for all alerts, essentially acts as an in-memory buffer to 
    store N alerts and dispatch them to a set of handlers.
    
    """
    alertBuffer = []
    bufferSize  = getattr(config, "bufferSize", 100)
    while True:
        alert = (yield)
        alertBuffer.append(alert)
        if len(alertBuffer) >= bufferSize: 
            for target in targets.values():
                target.send(alertBuffer)
            alertBuffer = []



@coroutine
def handleCritical(targets, config):
    """
    Handler for critical level alerts.
    
    """
    while True:
        alert = (yield)
        for target in targets.values():
            # sink's send() method expects list of Alert instances
            target.send([alert])

            
    
class Processor(object):
    """
    Alert handling processor that dispatches alerts into a handler pipeline.
    
    """
    def __init__(self, config):
        allFunctions = {}
        criticalFunctions = {}

        def getFunctions(config):
            r = {}
            for sink in config.sinks.listSections_():
                if sinksMap.has_key(sink):
                    sinkConfig = getattr(config.sinks, sink)
                    # create and store sink instance
                    r[sink] = sinksMap[sink](sinkConfig)
            return r

        # set up methods for the all alert handler which will buffer 
        # and flush to the sinks when the buffer is full
        allSection = config.all
        allFunctions = getFunctions(allSection)
        
        # set up handlers for critical alerts
        # critical alerts are passed straight through to the handlers
        # as they arrive, no buffering takes place
        criticalSection = config.critical
        criticalFunctions = getFunctions(criticalSection)
         
        pipelineFunctions = {
            "all": handleAll(allFunctions, allSection),
            "critical": handleCritical(criticalFunctions, criticalSection)
        }
        
        self.pipeline = dispatcher(pipelineFunctions, config)
        
        
    def __call__(self, alertData):
        """
        Inject a new alert into the processing pipeline
        The alert data will be plain JSON & needs to be converted into
        an alert instance before being dispatched to the pipeline
        
        """
        alert = Alert()
        alert.update(alertData)
        self.pipeline.send(alert)