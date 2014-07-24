"""
_WMWorkloadTools_

Define some generic tools used by the StdSpecs and WMWorkload
to validate arguments that modify a WMWorkload and/or WMTask.

Created on Jun 13, 2013

@author: dballest
"""
import json

from WMCore.DataStructs.LumiList import LumiList
from WMCore.WMException import WMException

class WMWorkloadToolsException(WMException):
    """
    _WMWorkloadToolsException_

    Exception thrown by the utilities in this module
    """
    pass

def makeLumiList(lumiDict):
    try:
        ll = LumiList(compactList = lumiDict)
        return ll.getCompactList()
    except:
        raise WMWorkloadToolsException("Could not parse LumiList")

def makeList(stringList):
    """
    _makeList_

    Make a python list out of a comma separated list of strings,
    throws a WMWorkloadToolsException if the input is not
    well formed. If the stringList is already of type list
    then it is return untouched
    """
    if isinstance(stringList, list):
        return stringList
    if isinstance(stringList, basestring):
        toks = stringList.lstrip(' [').rstrip(' ]').split(',')
        if toks == ['']:
            return []
        return[str(tok.strip(' \'"')) for tok in toks]
    raise WMWorkloadToolsException

def strToBool(string):
    """
    _strToBool_

    Convert the string to the matching boolean value:
    i.e. "True" to python True
    """
    if string == False or string == True:
        return string
    # Should we make it more human-friendly (i.e. string in ("Yes", "True", "T")?
    elif string == "True":
        return True
    elif string == "False":
        return False
    else:
        raise WMWorkloadToolsException()

def parsePileupConfig(mcPileup, dataPileup):
    """
    _parsePileupConfig_

    If the pileup config is defined as MCPileup and DataPileup
    then make sure we get the usual dictionary as
    PileupConfig : {'mc' : '/mc/procds/tier', 'data': '/minbias/procds/tier'}
    """
    pileUpConfig = {}
    if mcPileup is not None:
        pileUpConfig['mc'] = [mcPileup]
    if dataPileup is not None:
        pileUpConfig['data'] = [dataPileup]
    return pileUpConfig

def validateArguments(arguments, argumentDefinition):
    """
    _validateArguments_

    Validate a set of arguments against and argument definition
    as defined in StdBase.getWorkloadArguments. It returns
    an error message if the validation went wrong,
    otherwise returns None
    """
    for argument in argumentDefinition:
        optional = argumentDefinition[argument]["optional"]
        if not optional and argument not in arguments:
            return "Argument %s is required." % argument
        elif optional and argument not in arguments:
            continue
        validNull = argumentDefinition[argument]["null"]
        if not validNull and arguments[argument] is None:
            return "Argument %s can't be None" % argument
        elif validNull and arguments[argument] is None:
            continue
        try:
            argType = argumentDefinition[argument]["type"]
            argType(arguments[argument])
        except Exception:
            return "Argument %s type is incorrect in schema." % argument
        validateFunction = argumentDefinition[argument]["validate"]
        if validateFunction is not None:
            try:
                if not validateFunction(argType(arguments[argument])):
                    raise Exception
            except:
                # Some validation functions (e.g. Lexicon) will raise errors instead of returning False
                return "Argument %s doesn't pass validation." % argument
    return
