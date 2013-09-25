"""
Unlike ReqMgr1 defining Request and RequestSchema classes,
define just 1 class. Derived from Python dict and implementing
necessary conversion and validation extra methods possibly needed.
    
TODO/NOTE:
    'inputMode' should be removed by now (2013-07)
    
    since arguments validation #4705, arguments which are later
        validated during spec instantiation and which are not
        present in the request injection request, can't be defined
        here because their None value is not allowed in the spec.
        This is the case for e.g. DbsUrl, AcquisitionEra
        This module should probably define only absolutely
        necessary request parameters and not any optional ones.
    
"""


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
        
        # TODO
        # reassess if these CouchDB related details are necessary to be stored
        # in the request document! ReqMgr1 has all of these 3.
        self.setdefault("CouchURL", None) 
        # name of the ConfigCache database in Couch (historicaly misleading naming)
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