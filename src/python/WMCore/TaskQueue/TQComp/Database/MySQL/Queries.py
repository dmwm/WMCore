#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_Queries_

This module implements the mysql backend for the TQComp

"""

__revision__ = \
    "$Id: Queries.py,v 1.7 2009/09/29 14:25:41 delgadop Exp $"
__version__ = \
    "$Revision: 1.7 $"
__author__ = \
    "delgadop@cern.ch"

import threading
import logging

from WMCore.Database.DBFormatter import DBFormatter
from TQComp.CommonUtil import bindVals, bindWhere, commas, commasStr
import TQComp.Constants



########### MODULE FUNCTIONS ##########

######### CLASSES ###############

class Queries(DBFormatter):
    """
    _Queries_
    
    This module implements the mysql backend for the 
    TaskQueue.
    
    """
   
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)


# ------------ TASKS ----------------

    def addTasksBulk(self, taskList):
        """
        Inserts a bunch of tasks, with their attributes.
        The 'taskList' is a list of dicts, each as defined in
        TQComp.Apis.TQApiData.Task
        """      
        logging.debug("Queries.addBulk: %s entries" % (len(taskList)))
        sqlStr = """
         INSERT INTO tq_tasks(id, spec, sandbox, wkflow, type, reqs, req_se) 
         VALUES (:id, :spec, :sandbox, :wkflow, :type, :reqs, :req_se) 
        """ 
        # It seems that the DB iface requires a proper dict (rather than Task)
        taskList = map(dict, taskList)
        self.execute(sqlStr, taskList)


    def addTask(self, task):
#    def add(self, spec=None, sandbox=None, wkflow=None, type=0, pilot=None, state=0):
        """
        Inserts a new task, with its attributes.
        The 'task' is dict as defined in TQComp.Apis.TQApiData.Task
        """
#        logging.debug("Queries.add: %s %s %s %s %s" %(spec, sandbox, type, pilot, state))
        sqlStr = """
         INSERT INTO tq_tasks(id, spec, sandbox, wkflow, type, reqs, req_se) 
         VALUES (:id, :spec, :sandbox, :wkflow, :type, :reqs, :req_se) 
        """ 
        # It seems that the DB iface requires a proper dict (rather than Task)
        self.execute(sqlStr, dict(task))


#    def getTasksWithFilter(self, filter, fields=None, limit=None, \
#                           asDict=False, table='tq_tasks'):
#        """
#        Returns all tasks that match the specified filter. Filter must be
#        a dict containing valid fields as keys and the corresponding values
#        to match. The optional argument fields may contain a list of 
#        fields to return; otherwise, all are returned. The optional argument 
#        limit can be used to limit the maximum number of records returned.
#        If the optional argument 'asDict' is True, the result is returned as 
#        a dict with field names as keys; otherwise, result is a list of field
#        values.
#        """
#        
##        logging.debug("getTasksWithFilter running:", filter, fields, limit, asDict)
#        filterStr = limitStr = ""
#        fieldsStr = '*'
#    
#        if filter:
#            filterStr = "WHERE %s" % reduce(commas, map(bindWhere, filter))
#        if fields:
#            fieldsStr = "%s" % (reduce(commas, fields))
#        if limit:
#            limitStr = "LIMIT %s" % (limit)
#    
#        sqlStr = """
#        SELECT %s FROM tq_tasks %s %s
#        """ % (fieldsStr, filterStr, limitStr)
##        logging.debug("getTasksWithFilter sqlStr:", sqlStr)
#        
#        result = self.execute(sqlStr, filter)
#    
#        if asDict:
#            return self.formatDict(result)
#        else:
#            return self.format(result)


    def selectWithFilter(self, table, filter, fields=None, limit=None, \
                           asDict=False):
        """
        Returns all records that match the specified filter. Filter must be
        a dict containing valid fields as keys and the corresponding values
        to match. The optional argument fields may contain a list of 
        fields to return; otherwise, all are returned. The optional argument 
        limit can be used to limit the maximum number of records returned.
        If the optional argument 'asDict' is True, the result is returned as 
        a dict with field names as keys; otherwise, result is a list of field
        values.
        """
       
        logging.debug("selectWithFilter running:", table, filter, fields, \
                      limit, asDict)

        filterStr = limitStr = ""
        fieldsStr = '*'
    
        if filter:
            filterStr = "WHERE %s" % reduce(commas, map(bindWhere, filter))
        if fields:
            fieldsStr = "%s" % (reduce(commas, fields))
        if limit:
            limitStr = "LIMIT %s" % (limit)
    
        sqlStr = """
        SELECT %s FROM %s %s %s
        """ % (fieldsStr, table, filterStr, limitStr)

        logging.debug("selectWithFilter sqlStr:", sqlStr)
        
        result = self.execute(sqlStr, filter)
    
        if asDict:
            return self.formatDict(result)
        else:
            return self.format(result)


    def getTasksToMatch(self, filter, se, fields=None, limit=None, asDict=True):
        """
        Returns all tasks that match the specified filter and whose req_se
        contains the specified 'se'. Filter must be a dict containing valid 
        fields as keys and the corresponding values to match. The optional 
        argument fields may contain a list of fields to return; otherwise, 
        all are returned. The optional argument limit can be used to limit 
        the maximum number of records returned. If the optional argument 
        'asDict' is False, the result is returned as a list of field values,
        otherwise, result is a dict with field names as keys.
        """
        
#        logging.debug("getTasksToMatch running:", filter, se, fields, limit, asDict)
        filterStr = limitStr = ""
        fieldsStr = '*'
    
        if filter:
            filterStr = "WHERE %s" % reduce(commas, map(bindWhere, filter))
        if se:
            filterStr += " AND (find_in_set(:se, req_se) OR (req_se IS NULL));"
            if not 'se' in filter:
                filter['se'] = se
        if fields:
            fieldsStr = "%s" % (reduce(commas, fields))
        if limit:
            limitStr = "LIMIT %s" % (limit)
    
        sqlStr = """
        SELECT %s FROM tq_tasks %s %s
        """ % (fieldsStr, filterStr, limitStr)
#        logging.debug("getTasksWithFilter sqlStr:", sqlStr)
        
        result = self.execute(sqlStr, filter)
    
        if asDict:
            return self.formatDict(result)
        else:
            return self.format(result)


    def getTaskAtState(self, state):
        """
        Returns first task at given state 
        (to be replaced by a proper matching...).
        """
        sqlStr = """
        SELECT id, spec, sandbox FROM tq_tasks WHERE state = :state LIMIT 1
        """
        result = self.execute(sqlStr, {'state': state})
        return self.format(result)
    
   
    def lockTask(self, taskId):
        """
        Performs a "SELECT *" on the specified task using the 
        "FOR UPDATE" so that we are sure that nobody will
        modify or read the task until we comit our transaction.
        """
        self.logger.debug("Locking task: %s" % taskId)

        sqlStr = """
        SELECT * FROM tq_tasks WHERE id = :id FOR UPDATE
        """
        result = self.execute(sqlStr, {'id': taskId})
        return self.formatDict(result)

        
    def updateOneTask(self, taskId, vars):
        """
        Updates specified task with the fields in the given dict
        (if empty, then nothing is done). 
        """
        if vars:
            if 'state' in vars:
                sqlStr = """UPDATE tq_tasks SET current_state_time = 
                         CURRENT_TIMESTAMP, %s WHERE id=:id""" \
                         % reduce(commas, map(bindWhere, vars))
            else:
                sqlStr = """UPDATE tq_tasks SET %s WHERE id=:id""" \
                         % reduce(commas, map(bindWhere, vars))
            vars['id'] = taskId
            self.execute(sqlStr, vars)
    

    def updateTasks(self, idList, keys, vals):
        """
        Updates tasks whose id is in 'idList' with the keys in
        'keys' and the values in 'vals' (if empty, then nothing is done). 

        For same values to all updates (applied to different ids), 'vals'
        should be a list of the same length than 'keys' (otherwise, an
        exception is raised).
        
        For different bindings for each id, 'vals' should be a list
        of lists. The length of the external list must be the same of 
        'idList' and the list of each member list must be as that of
        'keys' (otherwise, an exception is raised).
        """
        bindings  = []
        stateStr = ""
        msg1 = "updateTasksBulk: Different length for value and id lists"
        msg2 = "updateTasksBulk: Different length for value and key lists"
        
        if 'state' in keys:
            stateStr = """current_state_time = CURRENT_TIMESTAMP,"""
        sqlStr = """UPDATE tq_tasks SET %s %s WHERE id=:id""" \
                 % (stateStr, reduce(commas, map(bindWhere, keys)))

        bindings = []
        if hasattr(vals[0], '__iter__'):
            if len(vals) != len(idList):
                raise ValueError(msg1)
            else: 
                for i in xrange(len(idList)):
                    if len(keys) != len(vals[i]):
                        raise ValueError(msg2)
                    mapping = dict(map(lambda x,y: (x,y), keys, vals[i]))
                    aux = mapping
                    aux['id'] = idList[i]
                    bindings.append(aux)
        else:
            if len(keys) != len(vals):
                raise ValueError(msg2)
            mapping = dict(map(lambda x,y: (x,y), keys, vals))
            for id in idList:
                aux = mapping.copy()
                aux['id'] = id
                bindings.append(aux)


        self.logger.debug("Update bulk - bindings: %s" % bindings)

        self.execute(sqlStr, bindings)



    def removeOneTask(self, taskid):
        """
        Removes the particular id from the list.
        """
        sqlStr = """
         DELETE FROM tq_tasks WHERE id = :id 
        """
        self.execute(sqlStr, {'id':taskid})



    def removeTasks(self, filter):
        """
        Removes the particular id from the list.
        """

        filterStr = ""
        if filter:
            filterStr = "WHERE %s" % reduce(commas, map(bindWhere, filter))

        sqlStr = """
        DELETE FROM tq_tasks %s
        """ % (filterStr)

        self.execute(sqlStr, filter)



    def count(self):
        """ 
        Counts the number of tasks in the list.
        """
        sqlStr = """
         SELECT COUNT(*) FROM tq_tasks 
        """
        result = self.execute(sqlStr, {})
        return self.formatOne(result)[0]
 
 
    def countRunning(self):
        """ 
        Counts the number of running tasks in the list.
        """
        state = TQComp.Constants.taskStates['Running']

        sqlStr = """
         SELECT COUNT(*) FROM tq_tasks WHERE state = :state
        """
#        """ % (state)
 
#        result = self.execute(sqlStr, {})
        result = self.execute(sqlStr, {'state': state})
        return self.formatOne(result)[0]
 
 
    def countQueued(self):
        """ 
        Counts the number of queued tasks in the list.
        """
        state = TQComp.Constants.taskStates['Queued']
        sqlStr = """
         SELECT COUNT(*) FROM tq_tasks WHERE state = :state
        """
#        """ % (state)
 
#        result = self.execute(sqlStr, {})
        result = self.execute(sqlStr, {'state': state})
        return self.formatOne(result)[0]


    def countDone(self):
        """ 
        Counts the number of correctly finished tasks in the list (not yet
        removed).
        """
        state = TQComp.Constants.taskStates['Done']
        sqlStr = """
         SELECT COUNT(*) FROM tq_tasks WHERE state = :state
        """
#        """ % (state)
 
#        result = self.execute(sqlStr, {})
        result = self.execute(sqlStr, {'state': state})
        return self.formatOne(result)[0]


    def countFailed(self):
        """ 
        Counts the number of failed tasks in the list.
        """
        state = TQComp.Constants.taskStates['Failed']
        sqlStr = """
         SELECT COUNT(*) FROM tq_tasks WHERE state = :state
        """
#        """ % (state)
 
#        result = self.execute(sqlStr, {})
        result = self.execute(sqlStr, {'state': state})
        return self.formatOne(result)[0]


    def countTasksBySeReq(self):
        """
        Counts the number of queued tasks grouped per the req_se field
        (list of valid SEs to run on, or NULL for any). The returned
        value is formatted as a list of dicts, with 'sites' as key for 
        the list representing the required SEs and 'tasks' as key whose
        value if the count of tasks.
        """
        state = TQComp.Constants.taskStates['Queued']
        sqlStr = """
         SELECT req_se as sites, COUNT(id) as tasks 
         FROM tq_tasks WHERE state = :state GROUP BY req_se
        """
 
        result = self.execute(sqlStr, {'state': state})
        return self.formatDict(result)


    def getStateOfTasks(self, taskIds = []):
        """
        Returns a dict with the provided task IDs as keys and their
        corresponding states as values.
        """

#        self.logger.debug("getStateOfTasks - taskIds: %s" % taskIds)

        if not taskIds:
            return {}

        # The following (with bind vars) should be fine, but for some reasons,
        # it only works for bind vars being integers, not string...

#        sqlStr = """
#         SELECT id, state FROM tq_tasks WHERE id IN :range
#        """ 
#        result = self.execute(sqlStr, {'range': taskIds})


        if len(taskIds) == 1:
            taskIds = "('%s')" % taskIds[0]
        else: 
            taskIds = "%s" % (tuple(taskIds),)

        sqlStr = """
         SELECT id, state FROM tq_tasks WHERE id IN %s
        """ % (taskIds)

        result = self.execute(sqlStr, {})
        result = self.format(result)

#        self.logger.debug("getStateOfTasks - result: %s" % result)

        return dict(result)

    
    def archiveTasksById(self, taskIds = []):
        """
        Archive all tasks whose Id is included in the 'taskIds' list
        (and exist in the queue): copy them from tq_tasks to tq_tasks_archive.
        """
        if taskIds:


            # The following (with bind vars) should be fine, but for some reasons,
            # it only works for bind vars being integers, not string...
#            sqlStr = """
#            DELETE FROM tq_tasks WHERE id IN :range
#            """ 
#            self.execute(sqlStr, {'range': taskIds})

            if len(taskIds) == 1:
                taskIds = "('%s')" % taskIds[0]
            else: 
                taskIds = "%s" % (tuple(taskIds),)
                
            sqlStr = """
            INSERT INTO tq_tasks_archive 
              SELECT * FROM tq_tasks WHERE id IN %s
            """ % (taskIds)

            result = self.execute(sqlStr, {})


    def removeTasksById(self, taskIds = []):
        """
        Remove all tasks whose Id is included in the 'taskIds' list
        (and exist in the queue).
        """
        if taskIds:


            # The following (with bind vars) should be fine, but for some reasons,
            # it only works for bind vars being integers, not string...
#            sqlStr = """
#            DELETE FROM tq_tasks WHERE id IN :range
#            """
#            self.execute(sqlStr, {'range': taskIds})

            if len(taskIds) == 1:
                taskIds = "('%s')" % taskIds[0]
            else:
                taskIds = "%s" % (tuple(taskIds),)

            sqlStr = """
            DELETE FROM tq_tasks WHERE id IN %s
            """ % (taskIds)

            result = self.execute(sqlStr, {})
           


# ------------ PILOTS ----------------
    
    def addPilot(self, vars):
        """
        Inserts a new pilot and returns the auto-assigned id.
        """
        if not vars:
            return None
            
        sqlStr = """INSERT INTO tq_pilots(%s) VALUES(%s)
        """ % (reduce(commas, vars), \
               reduce(commas, map(bindVals, vars)))
        
        self.execute(sqlStr, vars)

        sqlStr = """SELECT LAST_INSERT_ID() as id;"""
        
        result = self.execute(sqlStr, vars)

        return self.formatOne(result)[0]
    
    
    
    def updatePilot(self, pilotId, vars):
        """
        Updates pilot with given id, using the fields in 'vars'.
        If the field last_heartbeat is included with a value 
        of None, the CURRENT_TIMESTAMP is used for it.
        """
        if vars:
           
#            self.logger.debug("updatePilot - vars: %s" % vars)
            # Don't need the following lines since already by default a
            # NULL to a timestamp field already makes it CURRENT_TIMESTAMP
#            aux = vars
#            hbeatStr = ""
#            if 'last_heartbeat' in vars:
#                if vars['last_heartbeat'] == 0:
#                    aux = vars.copy()
#                    del aux['last_heartbeat']
#                    hbeatStr = "last_heartbeat = CURRENT_TIMESTAMP"
#                    if len(aux) > 0: 
#                        hbeatStr += ','
            
            sqlStr = """UPDATE tq_pilots SET %s WHERE id=:id""" \
                      % (reduce(commas, map(bindWhere, vars)))

#            self.logger.debug("updatePilot - sqlStr: %s" % sqlStr)
        
            vars['id'] = pilotId
            self.execute(sqlStr, vars)




#    def getPilotsWithFilter(self, filter, fields=None, limit=None, asDict=False):
#        """
#        Returns all pilots that match the specified filter. Filter must be
#        a dict containing valid fields as keys and the corresponding values
#        to match. The optional argument fields may contain a list of 
#        fields to return; otherwise, all are returned. The optional argument 
#        limit can be used to limit the maximum number of records returned.
#        If the optional argument 'asDict' is True, the result is returned as 
#        a list with field names as keys; otherwise, result is a list of field
#        values.
#        """
#        
#        filterStr = limitStr = ""
#        fieldsStr = '*'
#    
#        if filter:
#            filterStr = "WHERE %s" % reduce(commas, map(bindWhere, filter))
#        if fields:
#            fieldsStr = "%s" % (reduce(commas, fields))
#        if limit:
#            limitStr = "LIMIT %s" % (limit)
#    
#        sqlStr = """
#        SELECT %s FROM tq_pilots %s %s
#        """ % (fieldsStr, filterStr, limitStr)
#    
#        result = self.execute(sqlStr, filter)
#    
#        if asDict:
#            return self.formatDict(result)
#        else:
#            return self.format(result)


    def getPilotsAtHost(self, host, se, asDict=False):
        """ 
        Returns the pilots that are present in a given host (and se)
        and the cache directory for each of them.

        If the optional argument 'asDict' is True, the result is returned as 
        a list with field names as keys; otherwise, result is a list of field
        values.
        """
        sqlStr = """
         SELECT id as pilotId, cacheDir FROM tq_pilots 
         WHERE host = :host AND se = :se"""
 
        result = self.execute(sqlStr, {'host': host, 'se': se})
        
        if asDict:
            return self.formatDict(result)
        else:
            return self.format(result)


    def getPilotsPerHost(self, hostPattern):
        """ 
        Returns a list of tuples, one per matching host, including
        a list of pilots.
        """
        
        sqlStr = """
         SELECT se, host, id FROM tq_pilots
         WHERE host LIKE :hostPattern
         ORDER BY se, host
         """ 
 
        result = self.execute(sqlStr, {'hostPattern': hostPattern})
        return self.format(result)


    def getActivePilotsBySite(self):
        """
        Returns a list of lists. The inner lists have the 'se' string as 
        first element and the number of active pilots at that SE as second
        element. 
        """

        sqlStr = """
        SELECT tq_pilots.se, COUNT(DISTINCT tq_pilots.id) FROM tq_pilots JOIN tq_tasks ON
        tq_tasks.pilot = tq_pilots.id GROUP BY tq_pilots.se;
        """

        result = self.execute(sqlStr, {})
        return self.format(result)


    def getIdlePilotsBySite(self):
        """
        Returns a list of lists. The inner lists have the 'se' string as 
        first element and the number of active pilots at that SE as second
        element. 
        """

        sqlStr = """
        SELECT tq_pilots.se, COUNT(tq_pilots.id) FROM tq_pilots LEFT JOIN
        tq_tasks ON tq_tasks.pilot = tq_pilots.id WHERE tq_tasks.id IS NULL
        GROUP BY tq_pilots.se;
        """

        result = self.execute(sqlStr, {})
        return self.format(result)


    def countPilotMates(self, pilotId):
        """ 
        Returns the number of pilots that are present in a the same host
        (and se) that the pilot with the given id.
        """
#         SELECT id, cacheDir FROM tq_pilots 
#         WHERE host = :host AND se = :se
        sqlStr = """
         SELECT COUNT(id) FROM tq_pilots
         WHERE  (host, se) 
            = (SELECT host, se FROM tq_pilots WHERE id = :id)
        """
 
        result = self.execute(sqlStr, {'id': pilotId})
        return self.formatOne(result)[0]


    def removePilot(self, pilotId):
        """
        Removes one pilot from the tq_pilots table.
        """

        sqlStr = """
         DELETE FROM tq_pilots WHERE id = :id
        """
        result = self.execute(sqlStr, {'id': pilotId})


    def archivePilot(self, pilotId):
        """
        Copies the pilot tq_pilots record to the tq_pilots_archive table.
        """
        sqlStr = """
            INSERT INTO tq_pilots_archive 
              SELECT * FROM tq_pilots WHERE id = :id
            """
        result = self.execute(sqlStr, {'id': pilotId})


    def logPilotEvent(self, pilotId, event, info=None, taskId=None, errorCode=0):
        """
        Logs an event in the tq_pilot_log table
        """

        sqlStr = """
         INSERT INTO tq_pilot_log (pilot_id, task_id, event, error_code, info)
         VALUES (:pilot_id, :task_id, :event, :error_code, :info)
        """
        vars = {'pilot_id': pilotId, 'task_id': taskId, 'event': event,
                'error_code': errorCode, 'info': info}
        self.execute(sqlStr, vars)


    def getPilotLogs(self, pilotId, limit = None, fields = None, asDict = True):
        """
        Get the records in tq_pilot_log that correspond to the specified
        pilotId (or to all if None). If limit is not None, do not return
        more than those records. If fields is not None, but a list, return
        only the fields whose name is specified in that list (presence of
        non existing fields will produce an error). The last argument selects 
        whether the result must be a dict or a list of the values.
        """
        if fields:
            fieldsStr = "%s" % (reduce(commas, fields))
        else:
            fieldsStr = '*'

        sqlStr = """SELECT %s FROM tq_pilot_log""" % (fieldsStr)
        vars = {}
         
        if pilotId:
            sqlStr += """ WHERE pilot_id = :pilot_id"""
            vars = {'pilot_id': pilotId}

        sqlStr += """ ORDER BY insert_time DESC"""

        if limit:
            sqlStr += """ LIMIT %s""" % (limit)

        result = self.execute(sqlStr, vars)
        if asDict:
            return self.formatDict(result)
        else:
            return self.format(result)

 
    def checkPilotsTtl(self):
        """
        Returns pilots that have lived too long. Notice that this will
        never select pilots with a NULL ttl, which is what we want.
        """

        sqlStr = """
         SELECT id FROM tq_pilots WHERE TIMESTAMPDIFF(SECOND,
          ttl_time, CURRENT_TIMESTAMP()) > ttl;
        """
        vars = {}
        result = self.execute(sqlStr, vars)
        return self.format(result)


    def checkPilotsHeartbeat(self, hbValidity):
        """
        Returns pilots that have not reported for too long.
        """

        sqlStr = """
         SELECT id FROM tq_pilots WHERE TIMESTAMPDIFF(SECOND,
          last_heartbeat, CURRENT_TIMESTAMP()) > :validity;
        """
        vars = {'validity': hbValidity}
        result = self.execute(sqlStr, vars)
        return self.format(result)


# ------------ DATA ----------------

    def getDataPerHost(self, hostPattern):
        """ 
        Returns a list of tuples, one per matching host, including
        a list of held file guids and associated StorageElement.
        """
        
        sqlStr = """
         SELECT se, host, data FROM tq_hostdata
         WHERE host LIKE :hostPattern
         ORDER BY se, host
         """ 
#         SELECT tq_hostdata.host, tq_data.name 
#         FROM tq_hostdata INNER JOIN tq_data 
#         ON tq_hostdata.data = tq_data.id
#         WHERE tq_hostdata.host LIKE :hostPattern
#         ORDER BY tq_hostdata.host
 
        result = self.execute(sqlStr, {'hostPattern': hostPattern})
        return self.format(result)


# TODO: This will go away when we move to cache per host
    def getDataPerPilot(self, pilotPattern):
        """ 
        Returns a list of tuples, one per matching pilot, including
        a list of held file guids.
        """
        
        sqlStr = """
         SELECT pilot, data FROM tq_pilotdata
         WHERE pilot LIKE :pilotPattern
         ORDER BY pilot
         """ 
 
        result = self.execute(sqlStr, {'pilotPattern': pilotPattern})
        return self.format(result)


    def getCacheAtHost(self, host, se):
        """ 
        Returns the files that are present in a given host's cache.
        """
        sqlStr = """
         SELECT data FROM tq_hostdata 
         WHERE host = :host AND se = :se
        """ 
#        SELECT tq_data.name FROM tq_hostdata INNER JOIN tq_data
#        ON tq_hostdata.data = tq_data.id
#        WHERE tq_hostdata.host = :host"""

 
        result = self.execute(sqlStr, {'host': host, \
                              'se': se})
        return self.format(result)


# TODO: This will go away when we move to cache per host
    def getCacheAtPilot(self, pilot):
        """ 
        Returns the files that are present in a given pilot's cache.
        """
        sqlStr = """
         SELECT data FROM tq_pilotdata 
         WHERE pilot = :pilot
        """ 
        result = self.execute(sqlStr, {'pilot': pilot})
        return self.format(result)



    def addFilesBulk(self, dataList):
        """
        Inserts a bunch of data entries, if they do not exist in the DB (if 
        they do, they are skipped). The criteria to determine whether the 
        data exists is that the 'guid' field of the entries must be unique.
        The 'dataList' argument is a list of dicts, each containing information
        for a data entry (as the argument of addFile).
        """
#         INSERT IGNORE INTO tq_data(id, spec, sandbox, wkflow, type, reqs, req_se) 
#         VALUES (:id, :spec, :sandbox, :wkflow, :type, :reqs, :req_se) 
        logging.debug("Queries.addFilesBulk: %s entries" % (len(dataList)))
        
        if not dataList: return
        
        sqlStr = """
         INSERT IGNORE INTO tq_data(%s) VALUES(%s)
        """ % (reduce(commas, dataList[0]), \
               reduce(commas, map(bindVals, dataList[0])))
        self.execute(sqlStr, dataList)


    def addFile(self, vars):
        """
        Inserts a new data entry, if it does not exist in the DB (if it 
        exists, nothing is added). The criteria to determine whether the
        data exists is that the 'guid' field of the entries must be unique.
        """
        if not vars:
            return None

        sqlStr = """
         INSERT IGNORE INTO tq_data(%s) VALUES(%s)
        """ % (reduce(commas, vars), \
               reduce(commas, map(bindVals, vars)))
        
        self.execute(sqlStr, vars)

#        sqlStr = """SELECT LAST_INSERT_ID() as id;"""
#        result = self.execute(sqlStr, {})
#        return self.formatOne(result)[0]
    
    
    def addFileHost(self, pilot, data):
        """
        Inserts a new host<->data association, given the data guid and
        a pilot on the host. If the association exists already
        (the pair must be unique), the insert is skipped silently.
        """

        if (not data) or (not pilot): return
        
#             (SELECT host from tq_pilots WHERE id = :pilot), 
        sqlStr = """
         INSERT IGNORE INTO tq_hostdata(host, se, data) 
             SELECT host, se, :data FROM tq_pilots WHERE id = :pilot
        """ 
#         VALUES(
#             (SELECT host, se, FROM tq_pilots WHERE id = :pilot),
#             :data)
        
#        self.execute(sqlStr, {'host': host, 'data': data})
        self.execute(sqlStr, {'pilot': pilot, 'data': data})
  

# TODO: This will go away when we move to cache per host
    def addFilePilot(self, pilot, data):
        """
        Inserts a new pilot<->data association, given the data guid and
        the pilot. If the association exists already  (the pair must be 
        unique), the insert is skipped silently.
        """

        if (not data) or (not pilot): return
        
        sqlStr = """
         INSERT IGNORE INTO tq_pilotdata(pilot, data) VALUES (:pilot, :data)
        """ 
        self.execute(sqlStr, {'pilot': pilot, 'data': data})


    def addFileHostBulk(self, pilot, dataList):
        """
        Inserts a bunch of host<->data associations, given a list of data 
        guids and a pilot on the host. If the association exists already
        (the pair must be unique), the insert is skipped silently.
        The 'dataList' argument is a list of dicts, each containing only 
        one field with 'guid' as key.
        """
        logging.debug("Queries.addFileHostBulk: %s entries" % (len(dataList)))

        if not dataList: return
        
        sqlStr = """
         INSERT IGNORE INTO tq_hostdata(host, se, data) 
             SELECT host, se, :guid FROM tq_pilots WHERE id = %s
        """ % (pilot)

        self.execute(sqlStr, dataList)

    
# TODO: This will go away when we move to cache per host
    def addFilePilotBulk(self, pilot, dataList):
        """
        Inserts a bunch of pilot<->data associations, given a list of data 
        guids and the pilot. If the association exists already
        (the pair must be unique), the insert is skipped silently.
        The 'dataList' argument is a list of dicts, each containing only 
        one field with 'guid' as key.
        """
        logging.debug("Queries.addFilePilotBulk: %s entries" % (len(dataList)))

        if (not pilot) or (not dataList): return

        for i in dataList: 
            i['pilot'] =  pilot

        
        sqlStr = """
         INSERT IGNORE INTO tq_pilotdata(pilot, data) VALUES (:pilot, :guid)
        """ 

        self.execute(sqlStr, dataList)


    def removeFileHost(self, data, pilot):
        """
        Removes a host<->data association, given the data guid and 
        a pilot on the host. If 'data' is None, remove all data 
        in the cache of the host where that pilot is.
        """
        if not data:
           dataStr = ""
           vars = {'pilot': pilot}
        else:
           dataStr = "AND data=:data"
           vars = {'pilot': pilot, 'data': data}

        sqlStr = """
         DELETE FROM tq_hostdata 
         WHERE (host, se) =
               (SELECT host, se FROM tq_pilots WHERE id = :pilot)
         %s
        """  % (dataStr)
        
#        self.execute(sqlStr, {'host': host, 'data': data})
        self.execute(sqlStr, vars)
    
# TODO: This will go away when we move to cache per host
    def removeFilePilot(self, data, pilot):
        """
        Removes a pilot<->data association, given the data guid and the
        pilot. If 'data' is None, remove all data in the cache of the pilot.
        """
        if not data:
           dataStr = ""
           vars = {'pilot': pilot}
        else:
           dataStr = "AND data=:data"
           vars = {'pilot': pilot, 'data': data}

        sqlStr = """
         DELETE FROM tq_pilotdata WHERE pilot = :pilot %s
        """  % (dataStr)
        
        self.execute(sqlStr, vars)


    def removeLooseData(self):
        """
        Removes all data entries in the tq_data table, that are not 
        associated to any host in the tq_hostdata table.
        """
        sqlStr = """
         DELETE FROM tq_data 
         USING tq_data LEFT OUTER JOIN tq_hostdata 
           ON tq_data.guid = tq_hostdata.data 
         WHERE tq_hostdata.host IS NULL;
        """
        result = self.execute(sqlStr, {})
        return self.format(result)
 
 
# TODO: This will go away when we move to cache per host
    def removeLooseDataPilot(self):
        """
        Removes all data entries in the tq_data table, that are not 
        associated to any pilot in the tq_pilotdata table.
        """
        sqlStr = """
         DELETE FROM tq_data 
         USING tq_data LEFT OUTER JOIN tq_pilotdata 
           ON tq_data.guid = tq_pilotdata.data 
         WHERE tq_pilotdata.pilot IS NULL;
        """
        result = self.execute(sqlStr, {})
        return self.format(result)
 
# ------------ EXECUTE ----------------
 
 
    def execute(self, sqlStr, args):
        """"
        __execute__
        Executes the queries by getting the current transaction
        and dbinterface object that is stored in the reserved words of
        the thread it operates in.
        """
        myThread = threading.currentThread()
        currentTransaction = myThread.transaction
        return currentTransaction.processData(sqlStr, args) 



