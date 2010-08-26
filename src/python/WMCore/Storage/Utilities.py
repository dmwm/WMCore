#!/usr/bin/env python
"""
_Utilities_


Misc utils for stage out operations

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: Utilities.py,v 1.1 2009/06/18 21:14:34 meloam Exp $"


from IMProv.IMProvLoader import loadIMProvString
from IMProv.IMProvQuery import IMProvQuery


class NodeFinder:

    def __init__(self, nodeName):
        self.nodeName = nodeName
        self.result = None

    def __call__(self, nodeInstance):
        if nodeInstance.name == self.nodeName:
            self.result = nodeInstance

def findStageOutNode(workflow, nodeName):
    """
    _findStageOutNode_

    General util to find the stage out node named in the workflow

    """
    finder = NodeFinder(nodeName)
    workflow.payload.operate(finder)
    stageOutNode = finder.result
    return stageOutNode


def extractStageOutFor(cfgStr):
    """
    _extractStageOutFor_

    Get the list of node names to stage out files for in this job

    """
    if len(cfgStr.strip()) == 0:
        return []

    try:
        config = loadIMProvString(cfgStr)
    except Exception, ex:
        # Not an XML formatted string
        return []

    if config == None:
        return []


    query = IMProvQuery("/StageOutConfiguration/StageOutFor[attribute(\"NodeName\")]")
    nodelist = query(config)
    result = [ str(x) for x in nodelist if x.strip() != "" ]
    return result



def extractRetryInfo(cfgStr):
    """
    extractRetryInfo

    Extract retry configuration settings

    """
    result = {
        "NumberOfRetries" : 3,
        "RetryPauseTime" : 600,
        }

    if len(cfgStr.strip()) == 0:
        return result

    try:
        config = loadIMProvString(cfgStr)
    except Exception, ex:
        # Not an XML formatted string
        return result
    if config == None:
        return {}

    query = IMProvQuery("/StageOutConfiguration/NumberOfRetries[attribute(\"Value\")]")
    vals = query(config)
    if len(vals) > 0:
        value = vals[-1]
        value = int(value)
        result['NumberOfRetries'] = value

    query = IMProvQuery("/StageOutConfiguration/RetryPauseTime[attribute(\"Value\")]")
    vals = query(config)
    if len(vals) > 0:
        value = vals[-1]
        value = int(value)
        result['RetryPauseTime'] = value
    return result


def extractStageOutOverride(cfgStr):
    """
    _extractStageOutOverride_

    Extract an Override configuration from the string provided

    """
    if len(cfgStr.strip()) == 0:
        return {}

    try:
        override = loadIMProvString(cfgStr)
    except Exception, ex:
        # Not an XML formatted string
        return {}
    if override == None:
        return {}

    commandQ = IMProvQuery("/StageOutConfiguration/Override/command[text()]")
    optionQ = IMProvQuery("/StageOutConfiguration/Override/option[text()]")
    seNameQ = IMProvQuery("/StageOutConfiguration/Override/se-name[text()]")
    lfnPrefixQ = IMProvQuery("/StageOutConfiguration/Override/lfn-prefix[text()]")


    command = commandQ(override)
    if len(command) == 0:
        return {}
    else:
        command = command[0]

    seName = seNameQ(override)
    if len(seName) == 0:
        return {}
    else:
        seName = seName[0]

    lfnPrefix = lfnPrefixQ(override)
    if len(lfnPrefix) == 0:
        return {}
    else:
        lfnPrefix = lfnPrefix[0]


    option = optionQ(override)
    if len(option) > 0:
        option = option[0]
    else:
        option = None

    return {
        "command" : command,
        "se-name" : seName,
        "lfn-prefix" : lfnPrefix,
        "option" : option,
        }


def getStageOutConfig(workflow, nodeName):
    """
    _getStageOutConfig_

    backwards compatible wrapper method for extracting stage out
    settings from workflow node with given name

    """
    stageOutNode = findStageOutNode(workflow, nodeName)


    stageOutFor = extractStageOutFor(stageOutNode.configuration)
    override = extractStageOutOverride(
        stageOutNode.configuration)
    controls = extractRetryInfo(stageOutNode.configuration)

    if stageOutFor == []:
        # no stageout for list from config, treat as string of node names
        nodeNames = [ x.strip() for x in stageOutNode.configuration.split()
                      if x.strip() != "" ]
        stageOutFor = nodeNames

    return stageOutFor, override, controls





