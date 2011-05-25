#!/usr/bin/env python
# encoding: utf-8
"""
Processor.py

Alert Processor pipeline.
Provides a coroutine based handling system for alerts that can be used by a Reciever to process
Alert streams sent from Agent components.


Created by Dave Evans on 2011-03-02.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
import json
from WMCore.Alerts.ZMQ.Receiver import Receiver
from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sinks.CouchSink import CouchSink
from WMCore.Alerts.ZMQ.Sinks.EmailSink import EmailSink
from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink

def coroutine(func):
    """
    _coroutine_

    Decorator method used to prime coroutines

    """
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start




@coroutine
def dispatcher(targets, config):
    """
    _dispatcher_
    
    Function to dispatch an alert to a set of targets based on the level of the Alert
    
    targets arg should be a dict of coroutines to handle the appropriate level of alert
    and contain routines for handling "all" and "critical" alerts
    """
    criticalThreshold = getattr(config, "critical.level", 8)
    allThreshold      = getattr(config, "all.level", 0)
    while True:
        alert = (yield)
        if alert.level > allThreshold:
            targets['all'].send(alert)
        if alert.level > criticalThreshold:
            targets['critical'].send(alert)
@coroutine
def handle_all(targets, config):
    """
    _all_
    
    Handler for all alerts, essentially acts as an in-memory buffer to 
    store N alerts and dispatch them to a set of handlers
    """
    alert_buffer = []
    buffer_size  = getattr(config, "buffer_size", 100)
    while True:
        alert = (yield)
        alert_buffer.append(alert)
        if len(alert_buffer) >= buffer_size: 
            for target in targets.values():
                target.send( alert_buffer )
            alert_buffer = []


        


@coroutine
def handle_critical(targets, config):
    """
    _critical_
    
    Handler for critical level alerts
    """
    while True:
        alert = (yield)
        for target in targets.values():
            target.send( [alert] )
    
    

        
        
        

   

        


class PropagateSink(object):
    """
    _PropagateSink_
    
    Alert forwarder to another alert processor
    """        
    def __init__(self, config):
        self.config = config
    
    def send(self, alerts):
        """
        _send_
        
        handle list of alerts
        """
        print "PropagateSink", alerts
        
    
    
sinks_map = {
    "file"  : FileSink,
    "couch" : CouchSink,
    "email" : EmailSink,
    "propagate" : PropagateSink,
}
    
class Processor:
    """
    _Processor_
    
    Alert handling processor that dispatches alerts into a handler pipeline
    
    """
    def __init__(self, config):
        
           
        all_functions = {}
        critical_functions = {}
        # set up methods for the all alert handler which will buffer 
        # and flush to the sinks when the buffer is full
        all_section = config.all
        for sink in all_section.sinks.listSections_():
            if sinks_map.has_key(sink):
                all_functions[sink] = sinks_map[sink](getattr(all_section.sinks, sink) )
            
        # set up handlers for critical alerts
        # critical alerts are passed straight through to the handlers
        # as they arrive. 
        critical_section = config.critical
        for sink in critical_section.sinks.listSections_():
            if sinks_map.has_key(sink):
                critical_functions['file'] = sinks_map[sink](getattr(critical_section.sinks, sink))
        
        pipeline_functions = {
            "all" : handle_all(all_functions, all_section),
            "critical" : handle_critical(critical_functions, critical_section)
        }
        
        
        self.pipeline = dispatcher(pipeline_functions, config)
        
        
    def __call__(self, alertData):
        """
        _operator(alertData)_
        
        Inject a new alert into the processing pipeline
        The alert data will be plain JSON & needs to be converted into
        an alert instance before being dispatched to the pipeline
        """
        alert = Alert()
        alert.update(alertData)
        self.pipeline.send(alert)
        



