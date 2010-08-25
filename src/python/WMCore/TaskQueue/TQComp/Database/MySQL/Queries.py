#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_Queries_

This module implements the mysql backend for the TQComp

"""

__revision__ = \
    "$Id: Queries.py,v 1.3 2009/06/01 09:57:09 delgadop Exp $"
__version__ = \
    "$Revision: 1.3 $"
__author__ = \
    "delgadop@cern.ch"

import threading
import logging

from WMCore.Database.DBFormatter import DBFormatter
from TQComp.CommonUtil import bindVals, bindWhere, commas 
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
         INSERT INTO tq_tasks(spec, sandbox, wkflow, type, reqs) 
         VALUES (:spec, :sandbox, :wkflow, :type, :reqs) 
        """ 
        self.execute(sqlStr, taskList)


    def addTask(self, task):
#    def add(self, spec=None, sandbox=None, wkflow=None, type=0, pilot=None, state=0):
        """
        Inserts a new task, with its attributes.
        The 'task' is dict as defined in TQComp.Apis.TQApiData.Task
        """
#        logging.debug("Queries.add: %s %s %s %s %s" %(spec, sandbox, type, pilot, state))
        sqlStr = """
         INSERT INTO tq_tasks(spec, sandbox, wkflow, type, reqs) 
         VALUES (:spec, :sandbox, :wkflow, :type, :reqs) 
        """ 
        self.execute(sqlStr, task)



    def getTasksWithFilter(self, filter, fields=None, limit=None, asDict=False):
        """
        Returns all tasks that match the specified filter. Filter must be
        a dict containing valid fields as keys and the corresponding values
        to match. The optional argument fields may contain a list of 
        fields to return; otherwise, all are returned. The optional argument 
        limit can be used to limit the maximum number of records returned.
        If the optional argument 'asDict' is True, the result is returned as 
        a list with field names as keys; otherwise, result is a list of field
        values.
        """
        
#        logging.debug("getTasksWithFilter running:", filter, fields, limit, asDict)
        filterStr = limitStr = ""
        fieldsStr = '*'
    
        if filter:
            filterStr = "WHERE %s" % reduce(commas, map(bindWhere, filter))
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
    
    
    def updateOneTask(self, taskid, vars):
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
            vars['id'] = taskid
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
         SELECT COUNT(*) FROM tq_tasks WHERE state = %s
        """ % (state)
 
        result = self.execute(sqlStr, {})
        return self.formatOne(result)[0]
 
 
    def countQueued(self):
        """ 
        Counts the number of running tasks in the list.
        """
        state = TQComp.Constants.taskStates['Queued']
        sqlStr = """
         SELECT COUNT(*) FROM tq_tasks WHERE state = %s
        """ % (state)
 
        result = self.execute(sqlStr, {})
        return self.formatOne(result)[0]
 
 
    
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


#   def updateInsertPilot(self, pilotId, vars):
#       """
#       Inserts a new pilot or, if existing, updates it,with the 
#       fields in the given dict.
#       """
##            ON DUPLICATE KEY UPDATE tq_pilots SET %s WHERE id = :id
#       if vars:
#          sqlStr = """INSERT INTO tq_pilots(id, %s) VALUES(:id, %s)
#           ON DUPLICATE KEY UPDATE %s 
#          """ % (reduce(commas, vars), \
#                 reduce(commas, map(bindVals, vars)), \
#                 reduce(commas, map(bindWhere, vars)))
#       else:
#          sqlStr = """INSERT INTO tq_pilots(id) VALUES(:id)"""
#       
#       vars['id'] = pilotId
#       self.execute(sqlStr, vars)



    def getPilotsWithFilter(self, filter, fields=None, limit=None, asDict=False):
        """
        Returns all pilots that match the specified filter. Filter must be
        a dict containing valid fields as keys and the corresponding values
        to match. The optional argument fields may contain a list of 
        fields to return; otherwise, all are returned. The optional argument 
        limit can be used to limit the maximum number of records returned.
        If the optional argument 'asDict' is True, the result is returned as 
        a list with field names as keys; otherwise, result is a list of field
        values.
        """
        
        filterStr = limitStr = ""
        fieldsStr = '*'
    
        if filter:
            filterStr = "WHERE %s" % reduce(commas, map(bindWhere, filter))
        if fields:
            fieldsStr = "%s" % (reduce(commas, fields))
        if limit:
            limitStr = "LIMIT %s" % (limit)
    
        sqlStr = """
        SELECT %s FROM tq_pilots %s %s
        """ % (fieldsStr, filterStr, limitStr)
    
        result = self.execute(sqlStr, filter)
    
        if asDict:
            return self.formatDict(result)
        else:
            return self.format(result)



    def getPilotsAtHost(self, host, se, asDict=False):
        """ 
        Returns the pilots that are present in a given host (and se)
        and the cache directory for each of them.

        If the optional argument 'asDict' is True, the result is returned as 
        a list with field names as keys; otherwise, result is a list of field
        values.
        """
        sqlStr = """
         SELECT id, cacheDir FROM tq_pilots 
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
        (unique pair), the insert is skipped silently.
        """

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
    

    def removeLooseData(self):
        """
        Removes all data entries data are not associated to any host
        in the tq_hostdata table.
        """
        sqlStr = """
         DELETE FROM tq_data 
         USING tq_data LEFT OUTER JOIN tq_hostdata 
           ON tq_data.guid = tq_hostdata.data 
         WHERE tq_hostdata.host IS NULL;
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



