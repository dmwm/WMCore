#!/usr/bin/env python


__revision__ = "$Id: SetBlocksInjected.py,v 1.1 2009/08/13 23:58:46 meloam Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from WMComponent.PhEDExInjector.Database.MySQL.SetBlocksInjected import SetBlocksInjected as MySQLBase


class SetBlocksInjected(MySQLBase):
    """
    Identical to MySQL version for now

    """
