#!/usr/bin/env python
"""
Function library for use of different handlers dealing with pilots.
"""
__all__ = []
__revision__ = "$Id: TQRoutines.py,v 1.1 2009/08/11 14:09:26 delgadop Exp $"
__version__ = "$Revision: 1.1 $"

from traceback import extract_tb
import sys
import threading

# This is for the reschedule method
from Constants import taskStates




def rescheduleTasks(handler, taskList):
    """
    Reschedule the specified tasks (by Id).

    @param handler: a ref to the calling handler.
    @param taskList: list of dicts, each containing an 'id' field
    """

    # Extract ids
    idList = map(lambda x: x['id'], taskList)
        
# TODO: Need a more sophisticated logic for this?
#       This is just changing state of all tasks to queued (never abort?)

    # Update all tasks to be queued
    handler.queries.updateTasks(idList, ['state', 'pilot'], \
                             [taskStates['Queued'], None])



def finishPilot(handler, transaction, pilotId, reason = None):
    """
    Does all that is needed when a pilot is considered dead
    (by self-report, ttl expiration, or absence of heartbeat)
    The caller must pass a ref to a transaction object but not 
    start, rollback or commit it. This method will do any of those.

    The method returns a RemoteMsg-like dictionary object, the caller
    may directly return that to a remote client (if a ListenerHandler)
    or extract interesting information from it (anyone else).

    The strcture of the returned dict is the following:
        {'msgType': result, 'payload': fields}
        
    Where 'result' is either 'pilotShutdownAck' or 'Error', and 'fields'
    is a new dict possibly containing some additional textual information
    ('Error' or 'info' fields).

    @param handler: a ref to the calling handler.
    @param pilotId: id of the pilot to finish
    @param transaction: a ref to the transaction object of the caller
    @param reason: optional textual info about reason of pilot death 
    """

    handler.logger.debug("finishPilot: %s" % pilotId)

    if (reason == 'TtlDeath') or (reason == 'HeartbeatDeath'):
        logEvent = reason
    else:
        logEvent = 'PilotShutdown'
   
    fields = {}
    try:
        transaction.begin()
                
        # See if this was the last pilot on its own host
        # and if so, remove its cache from that host
        res = handler.queries.countPilotMates(pilotId)
        handler.logger.debug("countPilotMates: %s" % res)

        if res == 0:
            result = 'Error'
            fields = {'Error': 'Not registered pilot', \
                      'PilotId': pilotId}
            transaction.rollback()
            return {'msgType': result, 'payload': fields}

        if res == 1:
            handler.queries.removeFileHost(None, pilotId)

        # Now, if the data was not at other host,
        # remove it also from the tq_data table
        handler.queries.removeLooseData()

        # Check if there are tasks with this pilot
        # and if so, run the procedure for rescheduling/abort
        res = handler.queries.getTasksWithFilter( \
                  {'pilot': pilotId}, asDict = True)
        if res: 
            rescheduleTasks(handler, res)

        # Log the pilot end
        handler.queries.logPilotEvent(pilotId, logEvent, reason)
        
        # Finally, archive the pilot
        handler.queries.archivePilot(pilotId)
        handler.queries.removePilot(pilotId)

        # Give the result back
        fields['info'] = 'Done'
        result = 'pilotShutdownAck'

        # Commit
        transaction.commit()
      
    except:
        type, val, tb = sys.exc_info()
        transaction.rollback()
        messg = 'Error in finishPilot, due to: %s - %s '% (type, val)
        handler.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
        result = 'Error'
        fields = {'Error': messg}

    return {'msgType': result, 'payload': fields}


    
