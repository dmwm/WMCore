#!/usr/bin/env python
"""
_RequestMakerInterface_


Interface Definition for a RequestMaker implementation.

A RequestMaker Implementation should do the following.

- Instantiate with no args via a factory method
- Be callable on a RequestMakerSchema implementation
- Build WorkflowSpec instances
- Embed each WorkflowSpec in a RequestSpec instance
- Return a list of RequestSpec instances generated from the schema

"""
import WMCore.Lexicon
import time

class RequestSchema(dict):
    """
    _RequestSchema_

    Base class for a request schema.

    Contains minimum fields required by request database.
    Subclasses should add other required fields

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("RequestType", None)
        self.setdefault("RequestName", None)
        self.setdefault("RequestPriority", 0)
        self.setdefault("RequestNumEvents", None)
        self.setdefault("RequestSizeFiles", None)
        self.setdefault("AcquisitionEra", None)
        self.setdefault("Group", None)
        self.setdefault("Requestor", None)
        self.setdefault("ScramArch", "slc5_amd64_gcc434")
        self.setdefault("RequestDate", list(time.gmtime()[:6]))
        self.basicFields = self.keys()
        self.validateFields = []

    def validateSchema(self):
        """
        _validateSchema__

        """
        for field in self.basicFields:
            if self[field] == None:
                msg = "Missing setting for required field: %s\n" % field
                raise RuntimeError, msg
        #  //
        # // subclass
        #//
        self.validate()



    def validate(self):
        """
        _validate_

        Override to validate the arguments in the schema

        """
        msg = ""
        for field in self.validateFields:
            if self.get(field, None) == None:
                msg += "Required Field %s not provided for %s\n" % (field, type(self).__name__)
        if msg != "":
            raise RuntimeError(msg)

        for identifier in ['ScramArch', 'RequestName', 'Group', 'Requestor']:
            self.lexicon(identifier, WMCore.Lexicon.identifier)
        self.lexicon('CMSSWVersion', WMCore.Lexicon.cmsswversion)
        for dataset in ['InputDataset', 'OutputDataset']:
            self.lexicon(dataset, WMCore.Lexicon.dataset)

    def lexicon(self, field, validator):
        if self.get(field, None) != None:
            try:
                validator(self[field])
            except AssertionError:
                raise RuntimeError, "Bad value for %s" % field

    def __to_json__(self, thunker):
        """
        __to_json__

        This is here to prevent the serializer from attempting to serialize
    this object and adding a bunch of keys that couch won't understand.
        """
        jsonDict = {}
        for key in self.keys():
            jsonDict[key] = self[key]
        return jsonDict