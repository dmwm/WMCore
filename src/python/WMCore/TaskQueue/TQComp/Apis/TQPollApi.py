#!/usr/bin/env python
"""
API to poll the TQ queue.

It inherits the ability to connect to the TQ database 
from TQComp.Apis.TQApi.
"""

__all__ = []
__revision__ = "$Id: TQPollApi.py,v 1.3 2009/09/29 12:23:02 delgadop Exp $"
__version__ = "$Revision: 1.3 $"

import logging
import threading
import time

from TQComp.Apis.TQApi import TQApi


class TQPollApi(TQApi):
    """
    API to poll the TQ queue. 
    """

    def __init__(self, logger, tqRef, dbIface = None):
        """
        Constructor.
        """
        # Call our parent to set everything up
        TQApi.__init__(self, logger, tqRef, dbIface)


#    def poll(spec, wkflow=None, type=0, sandbox=None):
#        """
#        Poll what?
#        """

#        logging.debug('Inserting task: %s, %s, %s, %s' %\
#                     (spec, sandbox, wkflow, type))

#        # Insert job and its characteristics in the database
#        self.transaction.begin()
##        self.queries.XXX(spec, sandbox, wkflow, type)

#        self.transaction.commit()

