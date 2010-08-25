#!/usr/bin/env python
"""
_GetUnsubscribedDatasets_

Oracle implementation on GetUnsubscribedDatasets.
"""

__revision__ = "$Id: GetUnsubscribedDatasets.py,v 1.1 2010/04/01 19:54:09 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.PhEDExInjector.Database.MySQL.GetUnsubscribedDatasets import GetUnsubscribedDatasets as MySQLBase

class GetUnsubscribedDatasets(MySQLBase):
    pass
