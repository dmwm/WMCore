#!/usr/bin/env python
"""
_Utilities_

Common/useful utils.
Note that WMWorkload, WMTask and WMStep all depend on this module, so
dont import them in it.

"""


def findObjectTypeAboveNode(node, objType):
    """
    _findObjectTypeAboveNode_

    Given a config section (tree or not) traverse up the parent
    structure until finding the first entry containing an objectType
    setting that is set to the required type

    """
    if getattr(node, "objectType", None) == objType:
        return node
    if node._internal_parent_ref == None:
        return None
    return findObjectTypeAboveNode(node._internal_parent_ref, objType)


def findTaskAboveNode(node):
    """
    _findTaskAboveNode_

    Given a config section (tree or not) traverse up the parent
    structure until finding the first entry containing an objectType
    setting that is set to WMTask

    """
    return findObjectTypeAboveNode(node, "WMTask")

def findWorkloadAboveNode(node):
    """
    _findWorkloadAboveNode_

    Given a config section (tree or not) traverse up the parent
    structure until finding the first entry containing an objectType
    setting that is set to WMWorkload

    """
    return findObjectTypeAboveNode(node, "WMWorkload")



def stepIdentifier(stepHelper):
    """
    _stepIdentifier_

    Generate the /Workload/Task/Step id from the step instance
    given.

    """
    task = findTaskAboveNode(stepHelper.data)
    identifier = "%s/%s" % (task.pathName,
                            stepHelper.name())
    return identifier
