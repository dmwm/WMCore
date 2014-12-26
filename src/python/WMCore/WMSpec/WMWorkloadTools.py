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

class InvlaidSpecArgumentError(WMException):
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
    
def checkDBSUrl(dbsUrl):
    # use the import statement here since this is packed and used in RunTime code.
    # dbs client is not shipped with it.
    
    if dbsUrl:
        try:
            #from WMCore.Services.DBS.DBS3Reader import DBS3Reader
            #DBS3Reader(dbsUrl).dbs.serverinfo()
            from WMCore.Services.Requests import JSONRequests
            jsonSender = JSONRequests(dbsUrl)
            result = jsonSender.get("/serverinfo")
            if not result[1] == 200:
                raise WMWorkloadToolsException("DBS is not connected: %s : %s" % (dbsUrl, str(result)))
        except:
            raise WMWorkloadToolsException("DBS is not responding: %s" % dbsUrl)
    
    return True
    
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

def _validateArgument(argument, value, argumentDefinition):
    validNull = argumentDefinition[argument]["null"]
    if not validNull and value is None:
        raise InvlaidSpecArgumentError("Argument %s can't be None" % argument)
    elif validNull and value is None:
        return value
                    
    try:
        value = argumentDefinition[argument]["type"](value)        
    except Exception:
        raise InvlaidSpecArgumentError("Argument %s type is incorrect in schema." % argument)
    
    validateFunction = argumentDefinition[argument]["validate"]
    if validateFunction is not None:
        try:
            if not validateFunction(value):
                raise InvlaidSpecArgumentError("Argument %s doesn't pass the validation function." % argument)
        except Exception, ex:
            # Some validation functions (e.g. Lexicon) will raise errors instead of returning False
            raise InvlaidSpecArgumentError("Validation failed: %s, %s, %s" % (argument, value, str(ex)))
    return value

def validateArgumentsCreate(arguments, argumentDefinition):
    """
    _validateArguments_

    Validate a set of arguments against and argument definition
    as defined in StdBase.getWorkloadArguments. It returns
    an error message if the validation went wrong,
    otherwise returns None, this is used for spec creation 
    checks the whether argument is optional as well as validation
    """
    for argument in argumentDefinition:
        optional = argumentDefinition[argument]["optional"]
        if not optional and argument not in arguments:
            return "Argument %s is required." % argument
        elif optional and argument not in arguments:
            continue
        arguments[argument] = _validateArgument(argument, arguments[argument], argumentDefinition)

    return

def validateArgumentsUpdate(arguments, argumentDefinition):
    """
    _validateArgumentsUpdate_

    Validate a set of arguments against and argument definition
    as defined in StdBase.getWorkloadArguments. It returns
    an error message if the validation went wrong,
    otherwise returns None
    """
    for argument in argumentDefinition:
        optional = argumentDefinition[argument].get("assign_optional", True)
        if not optional and argument not in arguments:
            if argumentDefinition[argument].has_key("default"):
                arguments[argument] = argumentDefinition[argument]["default"]
            else:
                return "Argument %s is required." % argument
        elif optional and argument not in arguments:
            continue
        
    for argument in arguments:
        arguments[argument] = _validateArgument(argument, arguments[argument], argumentDefinition)
    return

def setArgumentsNoneValueWithDefault(arguments, argumentDefinition):
    """
    sets the default value if arguments value is specified as None
    """
    for argument in arguments:
        if arguments[argument] == None:
            argumentDefinition[argument]["default"]
    return

def loadSpecClassByType(specType):        
    factoryName = "%sWorkloadFactory" % specType
    mod = __import__("WMCore.WMSpec.StdSpecs.%s" % specType,
                     globals(), locals(), [factoryName])
    specClass = getattr(mod, factoryName)
    
    return specClass

def loadSpecByType(specType):        
    specClass = loadSpecClassByType(specType)
    return specClass()

