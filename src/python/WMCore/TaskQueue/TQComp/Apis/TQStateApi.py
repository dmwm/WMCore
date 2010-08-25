#!/usr/bin/env python
"""
API to query the TQ queue about its state.

It inherits the ability to connect to the TQ database 
from TQComp.Apis.TQApi.
"""

__all__ = []
__revision__ = "$Id: TQStateApi.py,v 1.2 2009/04/30 09:00:23 delgadop Exp $"
__version__ = "$Revision: 1.2 $"

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


    def getTasks(self, filter={}, fields=[], limit=None):
        """
        Returns the filtered contents of the tasks DB.
        
        The filter argument can be used to select the type of tasks
        to retrieve. It must be a dict containing fields as keys
        and the values they should have. If any of the keys does not
        correspond to an existing field, it will be ignored.

        The optional argument fields may contain a list of fields to 
        return. Otherwise, all are returned. The optional argument limit
        can be used to limit the maximum number of records returned.
        """
        filter2 = {}
        for key in filter.keys():
            if key in TASK_FIELDS:
                filter2[key] = filter[key]
                
        fields2 = []
        for field in fields:
            if field in TASK_FIELDS:
                fields2.append(field)
               
        if filter and (not filter2):
            logging.error('getTasks: Filter keys not valid: %s' % filter)
            logging.error('getTasks: Refusing to dump all entries')
            return None
            
        if fields and (not fields2):
            logging.error('getTasks: No valid field was requested: %s' % fields)
            logging.error('getTasks: Aborting query')
            return None
           
        if len(filter2) < len(filter):
            logging.warning('getTasks: Not all filter keys valid: %s' % filter)
            logging.warning('getTasks: Using filter: %s' % filter2)
        else:
            logging.debug('getTasks: Using filter: %s' % filter2)

        if len(fields2) < len(fields):
            logging.warning('getTasks: Not all fields valid: %s' % fields)
            logging.warning('getTasks: Requesting fields: %s' % fields2)
        else:
            logging.debug('getTasks: Requesting fields: %s' % fields2)

        # Perform query
#        self.transaction.begin()
        result = self.queries.getTasksWithFilter(filter2, fields2, limit)
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
