"""
Unlike ReqMgr1 defining Request and RequestSchema classes,
define just 1 class. Derived from Python dict and implementing
necessary conversion and validation extra methods possibly needed.

NOTE:
if practical, define here a sublist of necessary request fields to
    check, otherwise let it fail during validation in spec that
    some necessary field is missing
    
TODO/NOTE:
not sure what 'inputMode' request input argument is good for ... investigate

TODO/NOTE:
InputDataset vs InputDatasets - do we need both of these?    

"""

import WMCore.Lexicon

from RequestType import REQUEST_TYPES 



class RequestDataError(Exception):
    pass



class Request(dict):
    """
    Data Object representing a request.

    """

    def __init__(self):
        dict.__init__(self)
        # TODO
        # should discuss if list here all possible request arguments
        # although some make sense only for certain workloads or
        # only the common ones ...
        
        # these arguments are figured out automatically by ReqMgr,
        # ReqMgr fails to inject a request specifying any of these
        # arguments in the user input
        self.setdefault("RequestName", None)
        self.setdefault("RequestStatus", None)
        self.setdefault("Requestor", None)
        self.setdefault("RequestWorkflow", None)        
        self.setdefault("RequestDate", None),
        
        # TODO1
        # these values will contain full URL instead of the
        # database name up to no. So some of these values
        # will get deprecated and new ones will be introduced
        # holding the entire database URL
        # URL of the ReqMgr CouchDB server
        # TODO2
        # reassess if these CouchDB related details are necessary to be stored
        # in the request document! ReqMgr1 has all of these 3.
        self.setdefault("CouchURL", None) 
        # name of the ConfigCache database in Couch (historical misleading naming)
        self.setdefault("CouchDBName", None)
        # name of the main ReqMgr CouchDB database
        self.setdefault("CouchWorkloadDBName", None)
        self._automatic = self.keys()
                
        # normal input request arguments - to be present in the user
        # request input specification
        self.setdefault("RequestString", None)        
        self.setdefault("RequestType", None)
        self.setdefault("RequestPriority", None)
        self.setdefault("RequestNumEvents", None)
        self.setdefault("RequestSizeFiles", None)
        self.setdefault("AcquisitionEra", None)
        self.setdefault("Group", None)
        self.setdefault("OutputDatasets", [])
        # particular CMSSW version to run on
        self.setdefault("CMSSWVersion", None)
        # a list of possible CMSSW versions (both these arguments are necessary)
        self.setdefault("SoftwareVersions", [])
        self.setdefault("InputDatasets", [])
        self.setdefault("InputDatasetTypes", {})
        self.setdefault("SizePerEvent", 0)
        self.setdefault("PrepID", None)
        self.setdefault("DbsUrl", None)
        self.setdefault("ScramArch", None)
        self.setdefault("GlobalTag", None)
        self.setdefault("ConfigCacheID", None)
        self.setdefault("ConfigCacheUrl", None)
        self.setdefault("RunWhitelist", None)
        self.setdefault("Team", None)
        self.setdefault("TotalTime", None)
        self.setdefault("TimePerEvent", None)
        self.setdefault("Memory", None)
        self.setdefault("Campaign", None)
        # processing scenario, mutually exclusive with ConfigCacheID
        self.setdefault("Scenario", None)
        self.setdefault("EnableDQMHarvest", False)
        self.setdefault("EnableHarvesting", False)
                    
        
    def validate_automatic_args_empty(self):
        for arg in self._automatic:
            if self[arg]:
                msg = "ERROR: Request parameter %s can't be specified by the user." % arg
                raise RequestDataError(msg)
                        
        
    def validate(self):
        for identifier in ["ScramArch", "RequestName", "Group", "Requestor",
                           "RequestName", "Campaign", "ConfigCacheID"]:
            self.lexicon(identifier, WMCore.Lexicon.identifier)
        self.lexicon("CMSSWVersion", WMCore.Lexicon.cmsswversion)
        for dataset in ["InputDataset", "OutputDataset"]:
            self.lexicon(dataset, WMCore.Lexicon.dataset)
        if self["Scenario"] and self["ConfigCacheID"]:
            msg = "ERROR: Scenario and ConfigCacheID are mutually exclusive."
            raise RequestDataError(msg)
        if self["RequestType"] not in REQUEST_TYPES:
            msg = "ERROR: Request/Workload type '%s' not known." % self["RequestType"]
            raise RequestDataError(msg)
        
        # TODO
        # do also CMSSW versions validity, like in CheckIn.checkIn()
        """
        if not scramArch in versions.keys():
            m = ("Cannot find scramArch %s in ReqMgr (the one(s) available: %s)" %
                 (scramArch, versions))
            raise RequestCheckInError(m)
        for version in request.get('SoftwareVersions', []):
            if not version in versions[scramArch]:
                raise RequestCheckInError("Cannot find software version %s in ReqMgr for "
                                          "scramArch %s. Supported versions: %s" %
                                          (version, scramArch, versions[scramArch]))
        """
        
        # TODO
        # should be checking user/group membership? probably impossible, groups
        # is nothing that would be SiteDB ... (and there is no internal user
        # management here)
        
        # TODO
        # when is method is called, all automatic request arguments are
        # already figured out, should check that newly created RequestName
        # does not exist in Couch database, by any chance
        
        
    def lexicon(self, field, validator):
        if self.get(field, None) != None:
            try:
                validator(self[field])
            except AssertionError:
                msg = "Request argument validation failed, bad value for %s" % field
                raise RequestDataError(msg)


    def __to_json__(self, thunker):
        """
        This is here to prevent the serializer from attempting to serialize
        this object and adding a bunch of keys that couch won't understand.
        
        """
        json_dict = {}
        for key in self.keys():
            json_dict[key] = self[key]
        return json_dict