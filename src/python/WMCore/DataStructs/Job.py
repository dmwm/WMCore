#!/usr/bin/env python
"""
_Job_

Data object that describes a job

"""
__all__ = []
__revision__ = "$Id: Job.py,v 1.1 2008/07/07 09:40:13 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DataStructs.Pickleable import Pickleable 
from sets import Set
import datetime

class Job(Pickleable):
    def __init__(self, subscription=None, files=Set()):
        self.subscription = subscription
        self.file_set = files
        self.last_update = datetime.datetime.now()
