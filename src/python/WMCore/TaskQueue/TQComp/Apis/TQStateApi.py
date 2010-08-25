#!/usr/bin/env python
"""
API to query the TQ queue about its state.

It inherits the ability to connect to the TQ database 
from TQComp.Apis.TQApi.
"""

__all__ = []
__revision__ = "$Id: TQStateApi.py,v 1.1 2009/04/27 07:52:26 delgadop Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import threading
import time

from TQComp.Apis.TQApi import TQApi
from TQComp.Apis.TQApiData import TASK_FIELDS


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


    def getTasks(self, filter={}):
        """
        Returns the filtered contents of the tasks DB.
        
        The filter argument can be used to select the type of tasks
        to retrieve. It must be a dict containing fields as keys
        and the values they should have. If any of the keys does not
        correspond to an existing field, it will be ignored.
        """
        args = {}
        for key in filter.keys():
            if key in TASK_FIELDS:
                args[key] = filter[key]
               
        if filter and (not args):
            logging.error('getTasks: Filter keys not valid: %s' % filter)
            logging.error('getTasks: Refusing to dump all entries')
            return None
           
        if len(args) < len(filter):
            logging.warning('getTasks: Not all filter keys valid: %s' % filter)
            logging.warning('getTasks: Dumping tasks with filter: %s' % args)
        else:
            logging.debug('getTasks: Dumping tasks with filter: %s' % args)

        # Perform query
#        self.transaction.begin()
        result = self.queries.getTasksWithFilter(args)
        return result
#        self.transaction.commit()


    def countRunning(self):
        """
        Returns the number of tasks in the Running state
        """
        logging.debug('Getting number of running tasks')

        # Perform query
#        self.transaction.begin()
        result = self.queries.countRunning()
        return result
#        self.transaction.commit()


    def countQueued(self):
        """
        Returns the number of tasks in the Queued state
        """
        logging.debug('Getting number of queued tasks')

        # Perform query
#        self.transaction.begin()
        result = self.queries.countQueued()
        return result
#        self.transaction.commit()
