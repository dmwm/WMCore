"""
Alert Processor pipeline.
Provides a coroutine based handling system for alerts that can be used
by a Receiver to process Alert streams sent from Agent components.

"""

import sys
import logging
import traceback

from WMCore.Alerts.Alert import Alert
from WMCore.Alerts.ZMQ.Sinks.CouchSink import CouchSink
from WMCore.Alerts.ZMQ.Sinks.EmailSink import EmailSink
from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink
from WMCore.Alerts.ZMQ.Sinks.ForwardSink import ForwardSink
from WMCore.Alerts.ZMQ.Sinks.RESTSink import RESTSink


sinksMap = {
    "file": FileSink,
    "couch": CouchSink,
    "email": EmailSink,
    "forward": ForwardSink,
    "rest": RESTSink
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
    "soft" and "critical" alerts.

    Alerts are not duplicated, based on its level, an alerts ends up either
    in 'soft', resp. 'critical' sinks.

    """
    while True:
        alert = (yield)
        # alert level for critical threshold
        if alert.level >= config.critical.level:
            targets["critical"].send(alert)
            continue
        # alert level for soft threshold
        if alert.level >= config.soft.level:
            targets["soft"].send(alert)



@coroutine
def handleSoft(targets, config):
    """
    Handler for soft-level alerts, essentially acts as an in-memory buffer to
    store N alerts and dispatch them to a set of handlers.

    """
    alertBuffer = []
    bufferSize  = getattr(config, "bufferSize", 100)
    while True:
        alert = (yield)
        alertBuffer.append(alert)
        if len(alertBuffer) >= bufferSize:
            for target in targets.values():
                # if sending to a particular sink fails, the entire component
                # should remain functional
                # suboptimal to put this exception handling twice, but putting
                # it into dispatcher or later in __call__ undesirably catches StopIteration
                try:
                    target.send(alertBuffer)
                except Exception as ex:
                    trace = traceback.format_exception(*sys.exc_info())
                    traceString = '\n '.join(trace)
                    m = ("Sending alerts failed (soft) on %s, reason: %s\n%s" %
                         (ex, target.__class__.__name__, traceString))
                    logging.error(m)
            alertBuffer = []



@coroutine
def handleCritical(targets, config):
    """
    Handler for critical level alerts.

    """
    while True:
        alert = (yield)
        for target in targets.values():
            # if sending to a particular sink fails, the entire component
            # should remain functional
            # suboptimal to put this exception handling twice, but putting
            # it into dispatcher or later in __call__ undesirably catches StopIteration
            try:
                # sink's send() method expects list of Alert instances
                target.send([alert])
            except Exception as ex:
                trace = traceback.format_exception(*sys.exc_info())
                traceString = '\n '.join(trace)
                m = ("Sending alerts failed (critical) on %s, reason: %s\n%s" %
                     (ex, target.__class__.__name__, traceString))
                logging.error(m)



class Processor(object):
    """
    Alert handling processor that dispatches alerts into a handler pipeline.

    """
    def __init__(self, config):
        softFunctions = {}
        criticalFunctions = {}

        def getSinkInstance(sinkName, sinkConfig):
            sinkClass = sinksMap[sinkName]
            sinkInstance = None
            try:
                sinkInstance = sinkClass(sinkConfig)
            except Exception as ex:
                trace = traceback.format_exception(*sys.exc_info())
                traceString = '\n '.join(trace)
                m = ("Instantiating sink '%s' failed, reason: %s\n"
                     "%s\nconfig:\n%s" % (sinkClass.__name__, ex, traceString, sinkConfig))
                logging.error(m)
            return sinkInstance

        def getFunctions(config):
            r = {}
            for sink in config.sinks.listSections_():
                if sinksMap.has_key(sink):
                    sinkConfig = getattr(config.sinks, sink)
                    # create and store sink instance
                    logging.info("Instantiating '%s' sink ..." % sink)
                    sinkInstance = getSinkInstance(sink, sinkConfig)
                    if sinkInstance:
                        logging.info("Sink '%s' initialized." % sink)
                        r[sink] = sinkInstance
            return r

        # set up methods for the soft-level alert handler which will buffer
        # and flush to the sinks when the buffer is full
        logging.info("Instantiating 'soft' sinks ...")
        softFunctions = getFunctions(config.soft)

        # set up handlers for critical-level alerts
        # critical alerts are passed straight through to the handlers
        # as they arrive, no buffering takes place
        logging.info("Instantiating 'critical' sinks ...")
        criticalFunctions = getFunctions(config.critical)

        pipelineFunctions = {
            "soft": handleSoft(softFunctions, config.soft),
            "critical": handleCritical(criticalFunctions, config.critical)
        }
        self.pipeline = dispatcher(pipelineFunctions, config)
        logging.info("Initialized.")


    def __call__(self, alertData):
        """
        Inject a new alert into the processing pipeline
        The alert data will be plain JSON & needs to be converted into
        an alert instance before being dispatched to the pipeline

        """
        logging.debug("Processing incoming Alert data to sinks ...")
        alert = Alert()
        alert.update(alertData)
        self.pipeline.send(alert)
        logging.debug("Incoming Alert data processing done.")
