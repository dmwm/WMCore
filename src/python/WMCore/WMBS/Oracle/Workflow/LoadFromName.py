#!/usr/bin/env python
"""
_LoadFromName_

Oracle implementation of Workflow.LoadFromName
"""

from WMCore.WMBS.MySQL.Workflow.LoadFromName import LoadFromName \
    as LoadFromNameMySQL


class LoadFromName(LoadFromNameMySQL):
    pass
