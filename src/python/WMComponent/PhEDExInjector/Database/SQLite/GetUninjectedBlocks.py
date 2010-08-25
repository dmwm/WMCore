#!/usr/bin/env python


__revision__ = "$Id: GetUninjectedBlocks.py,v 1.1 2009/08/13 23:58:46 meloam Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from WMComponent.PhEDExInjector.Database.MySQL.GetUninjectedBlocks import GetUninjectedBlocks as MySQLBase


class GetUninjectedBlocks(MySQLBase):
    """
    Identical to MySQL version for now

    """
