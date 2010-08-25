#!/usr/bin/env python
"""
_WMWorkload_

Request level processing specification, acts as a container of a set
of related tasks.

"""
__revision__ = "$Id: WMWorkload.py,v 1.7 2009/09/24 20:14:46 sryu Exp $"
__version__ = "$Revision: 1.7 $"



from WMCore.Configuration import ConfigSection
from WMCore.WMSpec.ConfigSectionTree import findTop
from WMCore.WMSpec.Persistency import PersistencyHelper
from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper


def getWorkloadFromTask(taskRef):
    """
    _getWorkloadFromTask_

    Util to retrieve a Workload wrapped in a WorkloadHelper
    from a WMTask

    """
    nodeData = taskRef
    if isinstance(taskRef, WMTaskHelper):
        nodeData = taskRef.data

    topNode = findTop(nodeData)
    if not hasattr(topNode, "objectType"):
        msg = "Top Node is not a WM definition object:\n"
        msg += "Object has no objectType attribute"
        #TODO: Replace with real exception class
        raise RuntimeError, msg

    objType = getattr(topNode, "objectType")
    if objType != "WMWorkload":
        msg = "Top level object is not a WMWorkload: %s" % objType
        #TODO: Replace with real exception class
        raise RuntimeError, msg

    return WMWorkloadHelper(topNode)




class WMWorkloadHelper(PersistencyHelper):
    """
    _WMWorkloadHelper_

    Methods & Utils for working with a WMWorkload instance

    """
    def __init__(self, wmWorkload = None):
        self.data = wmWorkload
        
    def setSpecUrl(self, url):
        self.data.persistency.specUrl = url
        
    def specUrl(self):
        return self.data.persistency.specUrl
    #TODO: I am not sure using property function is better for simple accessor.
    #Ask Dave: either remove this do for other function for consistency
    specUrl = property(specUrl)
    
    def name(self):
        """
        _name_
        
        return name of the workload
        """
        return self.data._internal_name

    def owner(self):
        """
        _owner_
        return owner information
        """
        return self.data.owner
    
    def priority(self):
        """
        _priority_
        return priorty of workload
        """
        return self.data.request.priority
    
        
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
        pathName = "/%s/%s" % (self.name(), taskName)
        helper.setPathName(pathName)
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
        helper.setTopOfTree()
        self.addTask(helper)
        return helper



class WMWorkload(ConfigSection):
    """
    _WMWorkload_

    Request container

    """
    def __init__(self, name):
        ConfigSection.__init__(self, name)
        self.objectType = self.__class__.__name__
        #  //persistent data
        # //
        #//
        self.section_("persistency")
        self.persistency.specUrl = None
        #  //
        # // request related information
        #//
        self.section_("request")
        self.request.priority = None # what should be the default value 
        #  //
        # // owner related information
        #//
        self.section_("owner")

        #  //
        # // tasks
        #//
        self.section_("tasks")
        self.tasks.tasklist = []



def newWorkload(workloadName):
    """
    _newWorkload_

    Util method to create a new WMWorkload and wrap it in a helper

    """
    return WMWorkloadHelper(WMWorkload(workloadName))
