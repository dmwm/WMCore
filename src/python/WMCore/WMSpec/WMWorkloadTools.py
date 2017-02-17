"""
_WMWorkloadTools_

Define some generic tools used by the StdSpecs and WMWorkload
to validate arguments that modify a WMWorkload and/or WMTask.

Created on Jun 13, 2013

@author: dballest
"""
import json
import logging

from Utils.Utilities import makeList, strToBool
from WMCore.DataStructs.LumiList import LumiList
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException


def makeLumiList(lumiDict):
    try:
        if isinstance(lumiDict, basestring):
            lumiDict = json.loads(lumiDict)
        ll = LumiList(compactList=lumiDict)
        return ll.getCompactList()
    except:
        raise WMSpecFactoryException("Could not parse LumiList, %s: %s" % (type(lumiDict), lumiDict))


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
    """
    Validate a single argument against its definition in the spec
    """
    validNull = argumentDefinition["null"]
    if not validNull and value is None:
        raise WMSpecFactoryException("Argument %s can't be None" % argument)
    elif value is None:
        return value

    try:
        value = argumentDefinition["type"](value)
    except Exception:
        raise WMSpecFactoryException("Argument: %s: value: %s type is incorrect in schema." % (argument, value))

    _validateArgFunction(argument, value, argumentDefinition["validate"])
    return value


def _validateArgumentDict(argument, argValue, argumentDefinition):
    """
    Validate arguments that carry a dict value type
    """
    validNull = argumentDefinition["null"]
    if not validNull and None in argValue.values():
        raise WMSpecFactoryException("Argument %s can't be None" % argument)
    elif all(val is None for val in argValue.values()):
        return argValue

    for val in argValue.values():
        try:
            # sigh.. LumiList has a peculiar type validation
            if argument == 'LumiList':
                val = argumentDefinition["type"](argValue)
                break
            val = argumentDefinition["type"](val)
        except Exception:
            raise WMSpecFactoryException("Argument: %s, value: %s type is incorrect in schema." % (argument, val))

    _validateArgFunction(argument, argValue, argumentDefinition["validate"])
    return argValue


def _validateArgFunction(argument, value, valFunction):
    """
    Perform the validation function as in the argument definition
    """
    if valFunction:
        try:
            if not valFunction(value):
                raise WMSpecFactoryException(
                    "Argument %s, value: %s doesn't pass the validation function." % (argument, value))
        except Exception as ex:
            # Some validation functions (e.g. Lexicon) will raise errors instead of returning False
            logging.error(str(ex))
            raise WMSpecFactoryException("Validation failed: %s value: %s" % (argument, value))
    return


def _validateArgumentOptions(arguments, argumentDefinition, optionKey=None):
    """
    Check whether create or assign mandatory parameters were properly
    set in the request schema.
    """
    for arg, argValue in argumentDefinition.iteritems():
        optional = argValue.get(optionKey, True)
        if not optional and arg not in arguments:
            msg = "Validation failed: %s parameter is mandatory. Definition: %s" % (arg, argValue)
            raise WMSpecFactoryException(msg)
        # TODO this need to be done earlier then this function
        # elif optionKey == "optional" and not argumentDefinition[argument].get("assign_optional", True):
        #    del arguments[argument]
        # specific case when user GUI returns empty string for optional arguments
        elif arg not in arguments:
            continue
        elif isinstance(arguments[arg], dict):
            arguments[arg] = _validateArgumentDict(arg, arguments[arg], argValue)
        else:
            arguments[arg] = _validateArgument(arg, arguments[arg], argValue)
    return


def _validateInputDataset(arguments):
    inputdataset = arguments.get("InputDataset", None)
    dbsURL = arguments.get("DbsUrl", None)
    if inputdataset != None and dbsURL != None:
        # import DBS3Reader here, since Runtime code import this module and worker node doesn't have dbs3 client
        from WMCore.Services.DBS.DBS3Reader import DBS3Reader
        from WMCore.Services.DBS.DBSErrors import DBSReaderError
        try:
            DBS3Reader(dbsURL).checkDatasetPath(inputdataset)
        except DBSReaderError as ex:
            # we need to Wrap the exception to WMSpecFactoryException to be caught in reqmgr validation
            raise WMSpecFactoryException(str(ex))
    return


def validateInputDatasSetAndParentFlag(arguments):
    inputdataset = arguments.get("InputDataset", None)
    if strToBool(arguments.get("IncludeParents", False)):
        if inputdataset == None:
            msg = "IncludeParent flag is True but there is no inputdataset"
            raise WMSpecFactoryException(msg)
        else:
            dbsURL = arguments.get("DbsUrl", None)
            if dbsURL != None:
                # import DBS3Reader here, since Runtime code import this module and worker node doesn't have dbs3 client
                from WMCore.Services.DBS.DBS3Reader import DBS3Reader
                result = DBS3Reader(dbsURL).listDatasetParents(inputdataset)
                if len(result) == 0:
                    msg = "IncludeParent flag is True but inputdataset %s doesn't have parents" % (inputdataset)
                    raise WMSpecFactoryException(msg)
    else:
        _validateInputDataset(arguments)
    return


def validatePhEDExSubscription(arguments):
    """
    _validatePhEDExSubscription_

    Validate all the PhEDEx arguments provided during request
    creation and assignment.
    """
    for site in arguments.get("AutoApproveSubscriptionSites", []):
        if site.endswith('_MSS'):
            raise WMSpecFactoryException("Auto-approval to MSS endpoint is not allowed: %s" % site)
    if arguments.get("SubscriptionPriority", "Low") not in ["Low", "Normal", "High"]:
        raise WMSpecFactoryException("Invalid subscription priority: %s" % arguments["SubscriptionPriority"])
    if arguments.get("CustodialSubType", "Replica") not in ["Move", "Replica"]:
        raise WMSpecFactoryException("Invalid custodial subscription type: %s" % arguments["CustodialSubType"])
    if arguments.get("NonCustodialSubType", "Replica") not in ["Move", "Replica"]:
        raise WMSpecFactoryException("Invalid non custodial subscription type: %s" % arguments["NonCustodialSubType"])

    if 'CustodialGroup' in arguments and not isinstance(arguments["CustodialGroup"], basestring):
        raise WMSpecFactoryException("Invalid custodial PhEDEx group: %s" % arguments["CustodialGroup"])
    if 'NonCustodialGroup' in arguments and not isinstance(arguments["NonCustodialGroup"], basestring):
        raise WMSpecFactoryException("Invalid non custodial PhEDEx group: %s" % arguments["NonCustodialGroup"])
    if 'DeleteFromSource' in arguments and not isinstance(arguments["DeleteFromSource"], bool):
        raise WMSpecFactoryException("Invalid DeleteFromSource type, it must be boolean")

    return


def validateSiteLists(arguments):
    whiteList = arguments.get("SiteWhitelist", [])
    blackList = arguments.get("SiteBlacklist", [])
    whiteList = makeList(whiteList)
    blackList = makeList(blackList)
    res = (set(whiteList) & set(blackList))
    if len(res):
        msg = "Validation failed: The same site cannot be white and blacklisted: %s" % list(res)
        raise WMSpecFactoryException(msg)
    # store the properly formatted values (list instead of string)
    arguments["SiteWhitelist"] = whiteList
    arguments["SiteBlacklist"] = blackList
    return


def validateAutoGenArgument(arguments):
    autoGenArgs = ["TotalInputEvents", "TotalInputFiles", "TotalInputLumis", "TotalEstimatedJobs"]
    protectedArgs = set(autoGenArgs).intersection(set(arguments.keys()))

    if len(protectedArgs) > 0:
        raise WMSpecFactoryException("Shouldn't set auto generated params %s: remove it" % list(protectedArgs))
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
    validateAutoGenArgument(arguments)
    _validateArgumentOptions(arguments, argumentDefinition, "optional")
    validateInputDatasSetAndParentFlag(arguments)
    validatePhEDExSubscription(arguments)
    validateSiteLists(arguments)
    return


def validateArgumentsUpdate(arguments, argumentDefinition):
    """
    _validateArgumentsUpdate_

    Validate a set of arguments against and argument definition
    as defined in StdBase.getWorkloadArguments. It returns
    an error message if the validation went wrong,
    otherwise returns None
    """
    _validateArgumentOptions(arguments, argumentDefinition, "assign_optional")
    validatePhEDExSubscription(arguments)
    validateSiteLists(arguments)
    return


def validateArgumentsNoOptionalCheck(arguments, argumentDefinition):
    """
    _validateArgumentsNoOptionalCheck_

    Validate a set of arguments against and argument definition
    as defined in StdBase.getWorkloadArguments. But treats everything optional
    This is used for TaskChain request if some argument need to be overwritten
    It returns an error message if the validation went wrong,
    otherwise returns None
    """
    return _validateArgumentOptions(arguments, argumentDefinition)


def setAssignArgumentsWithDefault(arguments, argumentDefinition, checkList):
    """
    sets the default value if arguments value is specified as None
    """
    for argument in checkList:
        if not argument in arguments:
            arguments[argument] = argumentDefinition[argument]["default"]
    return


def setArgumentsWithDefault(arguments, argumentDefinition):
    """
    sets the default value if arguments value is specified as None
    """
    for argument in argumentDefinition:
        if argument not in arguments and "default" in argumentDefinition[argument]:
            arguments[argument] = argumentDefinition[argument]["default"]

    # set the Campaign default value to the same as AcquisitionEra if Campaign is not specified
    if not arguments.get("Campaign"):
        if ("AcquisitionEra" in arguments) and isinstance(arguments["AcquisitionEra"], basestring):
            arguments["Campaign"] = arguments["AcquisitionEra"]

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


def checkDBSURL(url):
    # import DBS3Reader here, since Runtime code import this module and worker node doesn't have dbs3 client
    from WMCore.Services.DBS.DBS3Reader import DBS3Reader
    return DBS3Reader(url).checkDBSServer()
