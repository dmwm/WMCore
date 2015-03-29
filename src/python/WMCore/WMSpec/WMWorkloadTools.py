"""
_WMWorkloadTools_

Define some generic tools used by the StdSpecs and WMWorkload
to validate arguments that modify a WMWorkload and/or WMTask.

Created on Jun 13, 2013

@author: dballest
"""
import logging

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Wrappers import JsonWrapper
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException

def makeLumiList(lumiDict):
    try:
        if isinstance(lumiDict, basestring):
            lumiDict = JsonWrapper.loads(lumiDict)
        ll = LumiList(compactList = lumiDict)
        return ll.getCompactList()
    except:
        raise WMSpecFactoryException("Could not parse LumiList, %s: %s" % (type(lumiDict), lumiDict))

def makeList(stringList):
    """
    _makeList_

    Make a python list out of a comma separated list of strings,
    throws a WMSpecFactoryException if the input is not
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
    raise WMSpecFactoryException("Can't convert to list %s" % stringList)

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
        raise WMSpecFactoryException("Can't convert to bool: %s" % string)

def _verifyDBSCall(dbsURL, uri):
    try:
        #from WMCore.Services.DBS.DBS3Reader import DBS3Reader
        #DBS3Reader(dbsUrl).dbs.serverinfo()
        from WMCore.Services.Requests import JSONRequests
        jsonSender = JSONRequests(dbsURL)
        result = jsonSender.get("/%s" % uri)
        if not result[1] == 200:
            raise WMSpecFactoryException("DBS is not connected: %s : %s" % (dbsURL, str(result)))
    except:
        raise WMSpecFactoryException("DBS is not responding: %s" % dbsURL)
    
    return result[0]
    
def checkDBSURL(dbsURL):
    # use the import statement here since this is packed and used in RunTime code.
    # dbs client is not shipped with it.
    
    if dbsURL:
        _verifyDBSCall(dbsURL, "serverinfo")
        return True
    
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
        raise WMSpecFactoryException("Argument %s can't be None" % argument)
    elif validNull and value is None:
        return value
                    
    try:
        value = argumentDefinition[argument]["type"](value)        
    except Exception:
        raise WMSpecFactoryException("Argument: %s: value: %s type is incorrect in schema." % (argument, value))
    
    validateFunction = argumentDefinition[argument]["validate"]
    if validateFunction is not None:
        try:
            if not validateFunction(value):
                raise WMSpecFactoryException("Argument %s: value: %s doesn't pass the validation function." % (argument, value))
        except Exception, ex:
            # Some validation functions (e.g. Lexicon) will raise errors instead of returning False
            logging.error(str(ex))
            raise WMSpecFactoryException("Validation failed: %s value: %s" % (argument, value))
    return value

def _validateArgumentOptions(arguments, argumentDefinition, optionKey):
    
    for argument in argumentDefinition:
        if optionKey == None:
            optional = True
        else:
            optional = argumentDefinition[argument].get(optionKey, True)
        if not optional and (argument not in arguments):
            raise WMSpecFactoryException("Validation failed: %s is mendatory %s" % (argument, 
                                                                argumentDefinition[argument]))
        #If assign_optional is set to false it need to be assigned later.
        #TODO this need to be done earlier then this function
        #elif optionKey == "optional" and not argumentDefinition[argument].get("assign_optional", True):
        #    del arguments[argument]
        # specific case when user GUI returns empty string for optional arguments
        elif optional and (argument not in arguments):
            continue
        elif optional and (argument in arguments) and (arguments[argument] == ""):
            del arguments[argument] 
        else:
            arguments[argument] = _validateArgument(argument, arguments[argument], argumentDefinition)
        return

def _validateInputDataset(arguments):
    inputdataset = arguments.get("InputDataset", None)
    dbsURL = arguments.get("DbsUrl", None)
    if inputdataset != None and dbsURL != None:
        result = _verifyDBSCall(dbsURL, "datasets?&dataset_access_type=*&dataset=%s" % inputdataset)
        if len(result) == 0:
            msg = "Inputdataset %s doesn't exist on %s" % (inputdataset, dbsURL)
            raise WMSpecFactoryException(msg)
    return

def validateInputDatasSetAndParentFlag(arguments):
    inputdataset = arguments.get("InputDataset", None)
    if strToBool(arguments.get("IncludeParents", False)):
        if inputdataset == None:
            msg = "Validation failed: IncludeParent flag is True but there is no inputdataset"
            raise WMSpecFactoryException(msg)
        else:
            dbsURL = arguments.get("DbsUrl", None)
            if dbsURL != None:
                result = _verifyDBSCall(dbsURL, "datasetparents?dataset=%s" % inputdataset)
                if len(result) == 0:
                    msg = "Validation failed: IncludeParent flag is True but inputdataset %s doesn't have parents" % (inputdataset)
                    raise WMSpecFactoryException(msg)
    else:
        _validateInputDataset(arguments)
    return
            
def validateArgumentsCreate(arguments, argumentDefinition):
    """
    _validateArguments_

    Validate a set of arguments against and argument definition
    as defined in StdBase.getWorkloadArguments. It returns
    an error message if the validation went wrong,
    otherwise returns None, this is used for spec creation 
    checks the whether argument is optional as well as validation
    """
    _validateArgumentOptions(arguments, argumentDefinition, "optional")
    validateInputDatasSetAndParentFlag(arguments)
    return

def validateArgumentsUpdate(arguments, argumentDefinition):
    """
    _validateArgumentsUpdate_

    Validate a set of arguments against and argument definition
    as defined in StdBase.getWorkloadArguments. It returns
    an error message if the validation went wrong,
    otherwise returns None
    """
    return _validateArgumentOptions(arguments, argumentDefinition, "assign_optional")

def validateArgumentsNoOptionalCheck(arguments, argumentDefinition):
    """
    _validateArgumentsNoOptionalCheck_

    Validate a set of arguments against and argument definition
    as defined in StdBase.getWorkloadArguments. But treats everything optional
    This is used for TaskChain request if some argument need to be overwritten
    It returns an error message if the validation went wrong,
    otherwise returns None
    """
    return _validateArgumentOptions(arguments, argumentDefinition, None)

def setAssignArgumentsWithDefault(arguments, argumentDefinition, checkList):
    """
    sets the default value if arguments value is specified as None
    """
    for argument in checkList:
        if not argument in arguments:
            arguments[argument] = argumentDefinition[argument]["default"]
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

