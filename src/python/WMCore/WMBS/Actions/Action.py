"""
WMBS BaseAction

A basic action is a thing that will run a SQL statement

A more complex one would be something that ran multiple SQL 
objects to produce a single output.
"""

from WMCore.Action import BaseAction as CoreBaseAction
from WMCore.Action import BoundAction as CoreBoundAction

class BaseAction(CoreBaseAction):
    name = "BaseAction"
    def __init__(self, logger):
        CoreBaseAction.__init__(self, package='WMCore.WMBS', logger=logger)

class BoundAction(CoreBoundAction):
    def __init__(self, logger):
        CoreBoundAction.__init__(package='WMCore.WMBS', logger=logger)