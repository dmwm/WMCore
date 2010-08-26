#!/usr/bin/env python

"""
_FlexPyMatchmaker_

This module contains a class that is able to match queued tasks with requesting
pilots. Any plugin implementing the interface that this class shows may be set
at configuration time as matcherPlugin. Otherwise, the default one in this module 
is used.
"""

__revision__ = "$Id: FlexPyMatchmaker.py,v 1.5 2009/12/16 18:09:05 delgadop Exp $"
__version__ = "$Revision: 1.5 $"
__author__ = "antonio.delgado.peris@cern.ch"


import sys 
import time
from Constants import RANK_EXPR, taskStates


class FlexPyMatchmaker(object):
    """ 
    _FlexPyMatchmaker_. This matchmaker is capable of evaluating stored
    requirement and rank expressions. It might be able to consider priority
    as well...
    """
    def __init__(self, params = {}):
        """
        Constructor. The 'params' argument can be used as a dict for any
        parameter (if needed).

        The required params are as follow:
           queries, logger
        """
        required = ["queries", "logger"]
        for param in required:
            if not param in params:
                messg = "FlexPyMatchmaker object requires params['%s']" % param
                raise ValueError(messg)

        self.queries = params["queries"]
        self.logger = params["logger"]
        

    def matchTask(self, pilot, limit = None):
        """
        Returns a list of the best matching tasks, or None if there is no
        task available.

        The argument 'pilot' must be a dict with all the information of the
        pilot. The optional argument limit may be used to indicate the maximum
        number of tasks to be returned (all matching if not set).
        """
        # TODO: The pilot may inform us about the files it owns, or we may look
        # into our database... whatever is better from the load point of view
        # (I guess it depends on the DB or the Listener being the bottleneck)
        # For now, assume the owned files are in a list under pilot['cache']

        self.logger.debug("Trying to match pilot: %s" % pilot)
        t0 = time.time()

        tasks = self.getTaskList(pilot['se'])
        self.logger.debug("Time spent in getTaskList: %1.2e" % (time.time() - t0))

        # Make it possible for reqs to include calls to QTIME()
        def calc_qtime():
#            print "QTIME:", time.time() - time.mktime(task['creat_time'].timetuple())
            return time.time() - time.mktime(task['creat_time'].timetuple())
        vars = pilot
#        self.logger.debug("vars: %s" % vars)
        vars['QTIME'] = calc_qtime
        # We eliminate the builtins to reduce eval's risk
        myglobals={'__builtins__': None}
       
        # Calculate all tasks that match this pilot
        matches = []
        t0 = time.time()
        for task in tasks:
#            if task['reqs']:
#                self.logger.debug("Task: %s, Eval: %s\n" % \
#                              (task, eval(task['reqs'], myglobals, vars)))
#            else:
#                self.logger.debug("Task: %s, reqs=None. Matching\n" %task)
            try:
                if (not task['reqs']) or (eval(task['reqs'], myglobals, vars)):
                    matches.append(task)
            except:
                ttype, val, tb = sys.exc_info()
                messg = "Error evaluating task (skipped): %s - %s" % (ttype, val)
                self.logger.critical(messg + ". Task: %s" % (task))
               
        ids = []
        for i in matches: ids.append(i['id'])
        self.logger.debug("Matching tasks: %s" % ids)
        self.logger.debug("Time spent in matching: %1.2e" % (time.time() - t0))
        
        if not matches:
            self.logger.debug("No matching task. Returning None")
            return None

        # Calculate the ranking for every matching task
        ranks = []
#        best = [-1, None]
        t0 = time.time()
        for match in matches:
            if not match['reqs']: 
                rank = 0
            else:
                vars = match.copy()
                vars['cache'] = pilot['cache']
                # RANK_EXPR is under our control, so we allow the use of _builtins_
                try:
                    rank = eval(RANK_EXPR, vars)
                except:
                    ttype, val, tb = sys.exc_info()
                    messg = "Error evaluating rank: %s - %s" % (ttype, val)
                    self.logger.critical(messg + ". Rank expr: %s" % (RANK_EXPR))
                    rank = 0
            ranks.append([rank, match])
#            self.logger.debug("Rank for task %s: %s\n" % (match['id'], rank))
#            if rank > best[0]:
#                best[0] = rank
#                best[1] = match

        def compfunc(x, y):
          if x[0] > y[0]: return -1
          if x[0] < y[0]: return 1
          return 0
        ranks.sort(compfunc)
#        self.logger.debug("Ranks for matching tasks: %s" % ranks)
# No need to reverse (the comp function does it fine already)
#        ranks.reverse()
        if not limit: 
            limit = len(ranks)
        else:
            limit = min(len(ranks), limit)
        best = []
        for i in xrange(limit):
            best.append(ranks[i][1])
        
#        self.logger.debug("Ranks for matching tasks: %s" % ranks)
        self.logger.debug("Time spent in ranking calc: %1.2e" % (time.time() - t0))

        # Return the best (or None)
#        self.logger.debug("Returning best task: %s, with rank: %s\n" % \
#                           (best[1]['id'], best[0]))

        if best:
            self.logger.debug("Returning best %i tasks. Best rank: %s\n" % \
                           (limit, best[0]))
        else:
            self.logger.debug("No best task to return")
        return best



    def getTaskList(self, se):
        """
        Returns the list of tasks currently in the queue. Each task is
        represented by a dictionary with its characteristics. The implementation
        of this method may retrieve the list from memory and only query the
        database is older than a predefined period of time, in order to increase
        performance and save load from the database.
        """
        # TODO: Implement in-memory myThread vars (taskList, timestamp, lock) to 
        # avoid querying the DB all the time

#        return self.queries.getTasksWithFilter({\
#                     'State': taskStates['Queued']}, asDict = True)
        return self.queries.getTasksToMatch({\
                     'State': taskStates['Queued']}, se)



