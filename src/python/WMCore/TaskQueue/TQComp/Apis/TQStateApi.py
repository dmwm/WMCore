#!/usr/bin/env python
"""
API to query the TQ queue about its state.

It inherits the ability to connect to the TQ database 
from TQComp.Apis.TQApi.
"""

__all__ = []
__revision__ = "$Id: TQStateApi.py,v 1.5 2009/08/11 14:09:26 delgadop Exp $"
__version__ = "$Revision: 1.5 $"

#import logging
import threading
import time

from TQComp.Apis.TQApi import TQApi
from TQComp.Apis.TQApiData import TASK_FIELDS, PILOT_FIELDS


class TQStateApi(TQApi):
    """
    API to query the TQ queue about its state.
    """

    def __init__(self, logger, tqRef, dbIface = None):
        """
        Constructor. Refer to the constructor of parent TQApi.
        """
        # Call our parent to set everything up
        TQApi.__init__(self, logger, tqRef, dbIface)


    def getStateOfTasks(self, taskIds = []):
        """
        Returns a dict with the provided task IDs as keys and their
        corresponding states as values.
        """

        self.transaction.begin()
        result = self.queries.getStateOfTasks(taskIds)
        self.transaction.commit()
        return result


    def getTasks(self, filter={}, fields=[], limit=None, asDict=False):
        """
        Returns the filtered contents of the tasks DB.
        
        The filter argument can be used to select the type of tasks to 
        retrieve. It must be a dict containing fields as keys and the values
        they should have. If any of the keys does not correspond to an 
        existing field, it will be ignored.

        The optional argument fields may contain a list of fields to return.
        Otherwise, all are returned. The optional argument limit can be used
        to limit the maximum number of records returned.

        If the optional argument 'asDict' is True, the result is a dict with
        field names as keys; otherwise, the result is a list of field values.
        """
        
#        self.logger.debug('%s: Starting' % ('getTasks'))
        
        return self.__getTable__(filter, fields, limit, asDict, 'tq_tasks', \
                     TASK_FIELDS, 'getTasks', self.queries.getTasksWithFilter)



    def getPilots(self, filter={}, fields=[], limit=None, asDict=False):
        """
        Returns the filtered contents of the pilots DB.
        
        The filter argument can be used to select the type of pilots to 
        retrieve. It must be a dict containing fields as keys and the values
        they should have. If any of the keys does not correspond to an 
        existing field, it will be ignored.

        The optional argument fields may contain a list of fields to return.
        Otherwise, all are returned. The optional argument limit can be used
        to limit the maximum number of records returned.

        If the optional argument 'asDict' is True, the result is a dict with
        field names as keys; otherwise, the result is a list of field values.
        """

#        self.logger.debug('%s: Starting' % ('getPilots'))
        
        return self.__getTable__(filter, fields, limit, asDict, 'tq_pilots', \
                  PILOT_FIELDS, 'getPilots', self.queries.getPilotsWithFilter)


    def __getTable__(self, filter, fields, limit, asDict, table, fList, \
                     caller, method):
        """
        Internal. For use of getTasks and getPilots.
        """
        
#        self.logger.debug('%s: Starting' % ('__getTable__'))
        filter2 = {}
        for key in filter.keys():
            if key in fList:
                filter2[key] = filter[key]
                
        fields2 = []
        for field in fields:
            if field in fList:
                fields2.append(field)
               
        if filter and (not filter2):
            self.logger.error('%s: Filter keys not valid: %s' % (caller, filter))
            self.logger.error('%s: Refusing to dump all entries' % (caller))
            return None
            
        if fields and (not fields2):
            self.logger.error('%s: No valid field requested: %s' % (caller, fields))
            self.logger.error('%s: Aborting query' % (caller))
            return None
           
        if len(filter2) < len(filter):
            self.logger.warning('%s: Not all filter keys valid: %s' % \
                            (caller, filter))
            self.logger.warning('%s: Using filter: %s' % (caller, filter2))
        else:
            self.logger.debug('%s: Using filter: %s' % (caller, filter2))

        if len(fields2) < len(fields):
            self.logger.warning('%s: Not all fields valid: %s' % (caller, fields))
            self.logger.warning('%s: Requesting fields: %s' % (caller, fields2))
        else:
            self.logger.debug('%s: Requesting fields: %s' % (caller, fields2))

        # Perform query
        self.transaction.begin()
        result = method(filter2, fields2, limit, asDict)
        self.transaction.commit()
        return result


    def getDataPerHost(self, hostPattern = "%"):
        """
        Returns a dict with pairs (se, host) as keys and list of 
        files (names) as values. Only hosts matching the provided 
        pattern are returned (all by default).
        """
        self.transaction.begin()
        res = self.queries.getDataPerHost(hostPattern)
        self.transaction.commit()

#        self.logger.debug("res: %s" % res)
        d = {}
        prev = ""
        for row in res:
            if (row[0], row[1]) == prev:
                d[(row[0], row[1])].append(row[2])
            else:
                d[(row[0], row[1])] = [row[2]]
            prev = (row[0], row[1])

        return d
        
    def getPilotsPerHost(self, hostPattern = "%"):
        """
        Returns a dict with pairs (se, host) as keys and list of pilots 
        (ids) as values. Only hosts matching the provided pattern are 
        returned (all by default).
        """
        self.transaction.begin()
        res = self.queries.getPilotsPerHost(hostPattern)
        self.transaction.commit()

#        self.logger.debug("res: %s" % res)
        d = {}
        prev = ""
        for row in res:
            if (row[0], row[1]) == prev:
                d[(row[0], row[1])].append(row[2])
            else:
                d[(row[0], row[1])] = [row[2]]
            prev = (row[0], row[1])

        return d
        



    def getPilotsAtHost(self, host, se, asDict=False):
        """ 
        Returns the pilots that are present in a given host (and se)
        and the cache directory for each of them.

        If the optional argument 'asDict' is True, the result is returned as 
        a list with field names as keys; otherwise, result is a list of field
        values.
        """
        self.transaction.begin()
        result = self.queries.getPilotsAtHost(host, se, asDict)
        self.transaction.commit()
        return result


    def countRunning(self):
        """
        Returns the number of tasks in the Running state
        """
        self.logger.debug('Getting number of running tasks')

        # Perform query
        self.transaction.begin()
        result = self.queries.countRunning()
        self.transaction.commit()
        return result


    def countQueued(self):
        """
        Returns the number of tasks in the Queued state
        """
        self.logger.debug('Getting number of queued tasks')

        # Perform query
        self.transaction.begin()
        result = self.queries.countQueued()
        self.transaction.commit()
        return result


    def getTaskCounts(self):
        """
        Returns a dict with task states as keys and the number of tasks at that
        state as values (regardless of assigned site or other considerations).
        Returned states at the moment is:
           running, queued, failed, done. 
        """
        result = {}

        self.transaction.begin()
        result['running'] = self.queries.countRunning()
        result['queued'] = self.queries.countQueued()
        result['failed'] = self.queries.countFailed()
        result['done'] = self.queries.countDone()
        self.transaction.commit()
        
        return result


    def getPilotCountsBySite(self):
        """
        Returns a dict with SEs as keys and dicts as values. These internal
        dicts have the strings 'ActivePilots' (for pilots running a task) 
        and 'IdlePilots' (for pilots registered but not running a task yet) 
        as keys and the corresponding number of pilots in those states as 
        values.

        Output format:
          {
            'se1': {'ActivePilots': 30, 'IdlePilots': 5}
            'se2': {'ActivePilots': 20, 'IdlePilots': 0}
          }

        """
        result = {}

        self.transaction.begin()
        active = self.queries.getActivePilotsBySite()
        idle = self.queries.getIdlePilotsBySite()
        self.transaction.commit()

        for i in active:
            result[i[0]] = {'ActivePilots': i[1]}
            result[i[0]]['IdlePilots'] = 0

        for i in idle:
            if not result.has_key(i[0]):
                result[i[0]] = {'ActivePilots': 0}
            result[i[0]]['IdlePilots'] = i[1]
        
        return result


    def countTasksBySeReq(self):
        """
        Counts the number of queued tasks grouped per the req_se field
        (list of valid SEs to run on, or NULL for any). The returned
        value is formatted as a list of dicts.
        """
        self.transaction.begin()
        counts = self.queries.countTasksBySeReq()
        for count in counts:
            if count['sites']: 
                count['sites'] = count['sites'].split(',')
        self.transaction.commit()

        return counts 

    def archiveTasksById(self, taskIds = []):
        """
        Archive all tasks whose Id is included in the 'taskIds' list
        (and exist in the queue): copy them from tq_tasks to 
        tq_tasks_archive, then  remove them from tq_tasks.
        """

        if taskIds:
            self.transaction.begin()
            self.queries.archiveTasksById(taskIds)
            self.queries.removeTasksById(taskIds)
            self.transaction.commit()

    def getPilotLogs(self, pilotId, limit = None):
        """
        Get the records in tq_pilot_log that correspond to the specified
        pilotId (or to all if None). If limit is not None, do not return
        more than those records.
        """
        self.transaction.begin()
        result = self.queries.getPilotLogs(pilotId, limit)
        self.transaction.commit()

        return result
        
