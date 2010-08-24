#!/usr/bin/env python
"""
_WMWorkload_

Request level processing specification, acts as a container of a set
of related tasks.

"""
__revision__ = "$Id: WMWorkload.py,v 1.1 2009/02/16 20:01:47 evansde Exp $"
__version__ = "$Revision: 1.1 $"



from WMCore.Configuration import ConfigSection
from WMCore.WMSpec.Persistency import PersistencyHelper
from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper

class WMWorkloadHelper(PersistencyHelper):
    """
    _WMWorkloadHelper_

    Methods & Utils for working with a WMWorkload instance

    """
    def __init__(self, wmWorkload = None):
        self.data = wmWorkload

    def getTask(self, taskName):
        """
        _getTask_

        Get Toplevel task by name

        """
        task = getattr(self.data.tasks, taskName, None)
        if task == None:
            return None
        return WMTaskHelper(task)


    def taskIterator(self):
        """
        generator to traverse top level tasks

        """
        for i in self.data.tasks.tasklist:
            yield self.getTask(i)

    def listAllTaskNames(self):
        """
        _listAllTaskNames_

        Generate a list of all known task names including
        tasks that are part of the top level tasks
        """
        result = []
        for t in self.taskIterator():
            result.extend(t.listNodes())
        return result


    def addTask(self, wmTask):
        """
        _addTask_

        Add a Task instance either naked or wrapped in a helper

        """
        task = wmTask
        if isinstance(wmTask, WMTaskHelper):
            task = wmTask.data
            helper = wmTask
        else:
            helper = WMTaskHelper(wmTask)
        taskName = helper.name()
        if taskName in self.listAllTaskNames():
            msg = "Duplicate task name: %s\n" % taskName
            msg += "Known tasks: %s\n" % self.listAllTaskNames()
            raise RuntimeError, msg
        self.data.tasks.tasklist.append(taskName)
        setattr(self.data.tasks, taskName, task)
        return




    def newTask(self, taskName):
        """
        _newTask_

        Factory like interface for adding a toplevel task to this
        workload

        """
        if taskName in self.listAllTaskNames():
            msg = "Duplicate task name: %s\n" % taskName
            msg += "Known tasks: %s\n" % self.listAllTaskNames()
            raise RuntimeError, msg
        task = WMTask(taskName)
        helper = WMTaskHelper(task)
        self.addTask(helper)
        return helper





class WMWorkload(ConfigSection):
    """
    _WMWorkload_

    Request container

    """
    def __init__(self, name):
        ConfigSection.__init__(self, name)
        #  //
        # // request related information
        #//
        self.section_("request")

        #  //
        # // owner related information
        #//
        self.section_("owner")

        #  //
        # // tasks
        #//
        self.section_("tasks")
        self.tasks.tasklist = []







