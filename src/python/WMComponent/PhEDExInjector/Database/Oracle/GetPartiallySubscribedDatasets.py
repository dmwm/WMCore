#!/usr/bin/env python
"""
_GetPartiallySubscribedDatasets_

Oracle implementation of PhEDExInjector.Database.GetPartiallySubscribedDatasets

Created on Oct 12, 2012

@author: dballest
"""

from WMComponent.PhEDExInjector.Database.MySQL.GetPartiallySubscribedDatasets import GetPartiallySubscribedDatasets \
     as MySQLGetPartiallySubscribedDatasets

class GetPartiallySubscribedDatasets(MySQLGetPartiallySubscribedDatasets):
    pass
