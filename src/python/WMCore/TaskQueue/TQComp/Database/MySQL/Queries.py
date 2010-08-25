#!/usr/bin/env python
#pylint: disable-msg=E1103

"""
_Queries_

This module implements the mysql backend for the TQComp

"""

__revision__ = \
    "$Id: Queries.py,v 1.1 2009/04/27 08:21:20 delgadop Exp $"
__version__ = \
    "$Revision: 1.1 $"
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
       

   def addBulk(self, taskList):
       """
       Inserts a bunch of tasks, with their attributes.
       The 'taskList' is a list of dicts, each as defined in
       TQComp.Apis.TQApiData.Task
       """      
#       for task in taskList:
#           task['pilot'] = None
#           task['state'] = 0
       logging.debug("Queries.addBulk: %s entries" % (len(taskList)))
       sqlStr = """
        INSERT INTO tq_tasks(spec, sandbox, wkflow, type) 
        VALUES (:spec, :sandbox, :wkflow, :type) 
       """ 
#        INSERT INTO tq_tasks(spec, sandbox, wkflow, type, pilot, state) 
#        VALUES (:spec, :sandbox, :wkflow, :type, :pilot, :state) 

       # Need a list of dicts for the bindings 
#       bindList = []
#       for task in taskList:
#          bindList.append(task.getDict)
#       bindList = {'spec': spec, 'sandbox': sandbox, 'wkflow': wkflow, 
#                   'type': type, 'pilot': pilot, 'state': state})
       self.execute(sqlStr, taskList)

   def add(self, task):
#   def add(self, spec=None, sandbox=None, wkflow=None, type=0, pilot=None, state=0):
       """
       Inserts a new task, with its attributes.
       The 'task' is dict as defined in TQComp.Apis.TQApiData.Task
       """
       logging.debug("Queries.add: %s" % (task))
#       task['pilot'] = None
#       task['state'] = 0
#       logging.debug("Queries.add: %s %s %s %s %s" %(spec, sandbox, type, pilot, state))
       sqlStr = """
        INSERT INTO tq_tasks(spec, sandbox, wkflow, type) 
        VALUES (:spec, :sandbox, :wkflow, :type) 
       """ 
#        INSERT INTO tq_tasks(spec, sandbox, wkflow, type, pilot, state) 
#        VALUES (:spec, :sandbox, :wkflow, :type, :pilot, :state) 
       self.execute(sqlStr, task)
#       self.execute(sqlStr, {'spec': spec, 'sandbox': sandbox, 'wkflow': wkflow, 
#                   'type': type, 'pilot': pilot, 'state': state})



   def getTasksWithFilter(self, filter, limit = None):
       """
       Returns all tasks that match the specified filter. Filter must be
       a dict containing valid fields as keys and the corresponding values
       to match. The optional argument limit can be used to limit the maximum
       number of records returned.
       """
       
       filterStr = limitStr = ""

       if limit:
           limitStr = "LIMIT %s" % (limit)
       if filter:
           filterStr = "WHERE %s" % reduce(commas, map(bindWhere, filter))

       sqlStr = """
       SELECT * FROM tq_tasks %s %s
       """ % (filterStr, limitStr)

       result = self.execute(sqlStr, filter)
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


   def updateTask(self, taskid, vars):
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
#          if pilot: 
#             sqlStr += "pilot = :pilot"
#             vars['pilot'] = pilot
#             if state: sqlStr += ","
#          if state: 
#             sqlStr += "host = :host"
#             vars['state']  = state
#          sqlStr += "WHERE id = :id"
          self.execute(sqlStr, vars)


   def updatePilot(self, pilotid, vars):
       """
       Inserts a new pilot or, if existing, updates it,with the 
       fields in the given dict.
       """
#            ON DUPLICATE KEY UPDATE tq_pilots SET %s WHERE id = :id
       if vars:
          sqlStr = """INSERT INTO tq_pilots(id, %s) VALUES(:id, %s)
           ON DUPLICATE KEY UPDATE %s 
          """ % (reduce(commas, vars), \
                 reduce(commas, map(bindVals, vars)), \
                 reduce(commas, map(bindWhere, vars)))
       else:
          sqlStr = """INSERT INTO tq_pilots(id) VALUES(:id)"""
       
       vars['id'] = pilotid
       self.execute(sqlStr, vars)


   def removeTask(self, taskid):
       """
       Removes the particular id from the list.
       """
       sqlStr = """
        DELETE FROM tq_tasks WHERE id = :id 
       """
       self.execute(sqlStr, {'id':taskid})
       

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



