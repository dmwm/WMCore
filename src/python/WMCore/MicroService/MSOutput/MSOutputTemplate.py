"""
File       : MSOutputTemplate.py
Description: Provides a document Template for MSOutput MicroServices
"""

# futures
from __future__ import division, print_function

from future.utils import viewitems
from builtins import str, bytes
from time import time
from copy import deepcopy
from WMCore.MicroService.Tools.Common import isRelVal


class MSOutputTemplate(dict):
    """
    A simple template to represent the objects stored in Mongodb for MSOutput
    Its purpose should be to serve as a bidirectional representation of the
    objects stored in Mongodb. The instances of this class should be used as a
    buffer for both inbound (reading from the database) and outbound (writing to
    database) documents. And it must have all the methods needed to set or update
    various fields in the object. So that we assure the uniformity of all documents.
    """
    # TODO:
    #    to implement document mutex in a model similar to the one described here:
    #    https://stackoverflow.com/questions/32728670/mutex-with-mongodb
    #    ```
    #    db.collection.update({done: false, taken: false},{$set: {taken: true, takenBy: myIp}});
    #    ```

    # TODO:
    #    as soon as any change in the instantiated object happens ( any call of
    #    a set method) the same change must be updated in Mongodb too -
    #    this may happen from outside or from the object itself. The former method
    #    keeps the document independent from the underlying Database technology

    def __init__(self, doc, **kwargs):
        super(MSOutputTemplate, self).__init__(**kwargs)

        # producerDoc has to be used to label what is the source of our input document,
        # such as:
        #   True: we are passing a reqmgr2 request dictionary
        #   False: we are passing an already formatted mongoDB record
        self.producerDoc = kwargs.pop("producerDoc", True)
        self.required = ['_id', 'RequestName', 'RequestType', 'CreationTime']
        self.allowEmpty = ['OutputDatasets']
        self.allowedStatus = ["pending", "done"]

        myDoc = dict()
        myDoc['CreationTime'] = int(time())
        for tup in self.docSchema():
            if tup[0] in doc:
                myDoc[tup[0]] = deepcopy(doc[tup[0]])
            else:
                myDoc.update({tup[0]: tup[1]})

        # if any object attribute is passed as a **kwargs parameter then
        # overwrite the equivalent parameter which was coming with the doc
        self._checkAttr(myDoc, update=True, throw=True, **kwargs)

        if self.producerDoc:
            # then it's a ReqMgr2 request dictionary. Call the setters
            self._setOutputMap(doc, myDoc)

        # enforce a full check on the final document
        self._checkAttr(myDoc, update=False, throw=True, **myDoc)

        # final validation:
        if self._checkValid(myDoc, throw=True):
            self.update(myDoc)
        if self.producerDoc:
            # finally, set whether it's Release Validation workflow or not
            self._setRelVal(doc)

    def docSchema(self):
        """
        Return the data schema for a record in MongoDB.
        It's a tuple where:
        * 1st element: is the key name / attribute in the request
        * 2nd element: is the default value
        * 3rd element: is the expected data type

        Template format:
            {
            "_id": "ReqName",
            "RequestName": "ReqName",
            "Campaign": "Top level campaign name",
            "CreationTime": integer timestamp,
            "LastUpdate": integer timestamp,
            "IsRelVal": (True|False),
            "OutputDatasets": ["list of output datasets"],
            "OutputMap": [{'Campaign': 'campaign name',
                           'Dataset': 'output dataset name',
                           'Copies': 1,
                           'DiskDestination': "",
                           'TapeDestination': "",
                           'DiskRuleID': "",
                           'TapeRuleID': ""},
                          {'Campaign': 'another (or the same) campaign name',
                           'Dataset': 'another output dataset name',
                           'Copies': 1,
                           ...}],
                    "TransferStatus": "pending"|"done,
            "RequestType": ""
            }
        :return: a list of tuples
        """
        docTemplate = [
            ('_id', None, (bytes, str)),
            ('RequestName', None, (bytes, str)),
            ('RequestType', "", (bytes, str)),
            ('Campaign', [], (bytes, str)),
            ('CreationTime', int(time()), int),
            ('LastUpdate', None, int),
            ('IsRelVal', False, bool),
            ('OutputDatasets', [], list),
            ('OutputMap', [], list),
            ('TransferStatus', "pending", (bytes, str))]
        return docTemplate

    def outputMapSchema(self):
        """
        Return the data schema for the OutputMap attribute in the MongoDB record.
        It's a tuple where:
        * 1st element: is the key name / attribute name
        * 2nd element: is the default value
        * 3rd element: is the expected data type

        Template format:
            {'Campaign': u'RunIIAutumn18DRPremix',
             'Dataset': u'/Pseudoscalar2HDM_MonoZLL_mScan_mH-500_ma-300/DMWM-TC_PreMix_khurtado_TC_PreMix-v11/AODSIM',
             'DatasetSize': 0,
             'Copies': 1,
             'DiskDestination': "",
             'TapeDestination': "",
             'DiskRuleID': "",
             'TapeRuleID': ""}
        :return: a list of tuples
        """
        outMapTemplate = [
            ('Campaign', "", (bytes, str)),
            ('Dataset', "", (bytes, str)),
            ('DatasetSize', 0, int),
            ('Copies', 1, int),
            ('DiskDestination', "", (bytes, str)),
            ('TapeDestination', "", (bytes, str)),
            ('DiskRuleID', "", (bytes, str)),
            ('TapeRuleID', "", (bytes, str))]
        return outMapTemplate

    def _checkAttr(self, myDoc, update=False, throw=False, **kwargs):
        """
        Basically checks everything given in **kwargs against the document schema
        and if it passes the check then it is absorbed in myDoc or just the
        result from the check is returned depending on the flags given
        :myDoc:       The document where the valid keys to be copied
        :**kwarg:     The source set of kwargs whose values to be checked
        :update:      Update flag:
                      - If True the keys from **kwargs will be copied to myDoc
                      - If False only the consistency checks will be performed
        :throw:       Error handling flag:
                      - If True upon a failed check an Error will be thrown
                      - If False only bool value from the checks will be returned
        """

        # check if we can fit all the arguments provided through **kwargs
        for kw in kwargs:
            found = False
            typeok = False
            for tup in self.docSchema():
                if kw == tup[0]:
                    found = True
                    # NOTE: Here we can allow more than one type per field if we
                    #       set them as a tuple of types eg. (str, unicode)
                    if isinstance(kwargs[kw], tup[2]) or kwargs[kw] is None:
                        typeok = True
                        if update:
                            # NOTE: Here we may consider deepcopy
                            myDoc[kw] = deepcopy(kwargs[kw])
            if not found:
                # NOTE: We can raise an error here and decide if we want to drop
                #       the whole document or we can jut ignore the current field
                # NOTE: We should keep in mind that raising an error from the object
                #       Creates a reference which will stay outside the normal
                #       scope of the object (until not caught) which may prevent
                #       GC cleaning the object
                msg = "ERROR: Unrecognized parameter: {}: {}".format(kw, kwargs[kw])
                if throw:
                    raise KeyError(msg)
                else:
                    return False
            if not typeok:
                # NOTE: Same as above
                msg = "ERROR: Wrong type: {} for parameter: {}: {}".format(
                    type(kwargs[kw]),
                    kw,
                    kwargs[kw])
                if throw:
                    raise TypeError(msg)
                else:
                    return False
        return True

    def _checkValid(self, myDoc, throw=False):
        """
        An internal method to be used in order to check for some mandatory fields
        before announcing the created document for valid
        """
        valid = []
        missing = []
        for mandField in self.required:
            if mandField in myDoc and myDoc[mandField]:
                valid.append(True)
            else:
                valid.append(False)
                missing.append(mandField)

        for mandField in self.allowEmpty:
            if mandField in myDoc:
                valid.append(True)
            else:
                valid.append(False)
                missing.append(mandField)

        if not all(valid):
            # NOTE: Same as above
            msg = "ERROR: Missing Mandatory Fields: {} for: {}".format(missing, myDoc)
            if throw:
                raise KeyError(msg)
            else:
                return False
        return True

    def setKey(self, key, value):
        """
        A method to be used for setting a key in the document
        """
        if key == "TransferStatus":
            self.setTransferStatus(value)
        else:
            myDoc = {key: value}
            if self._checkAttr(myDoc, throw=True, update=False, **myDoc):
                self.update(myDoc)

    def _setRelVal(self, myDoc):
        """
        Evaluates whether it's a release validation request, if so, set the flag to True
        :param myDoc: the request dictionary
        """
        if isRelVal(myDoc):
            self.setKey('IsRelVal', True)

    def setTransferStatus(self, newStatus):
        """
        Updates the TransferStatus attribute value
        """
        if newStatus not in self.allowedStatus:
            raise RuntimeError("TransferStatus: '{}' is not supported. Use: {}".format(newStatus,
                                                                                       self.allowedStatus))
        self.update(dict(TransferStatus=newStatus))

    def updateDoc(self, myDoc, throw=False):
        """
        A method to be used for updating the document fields from a dictionary
        """
        if self._checkAttr(myDoc, throw=throw, update=False, **myDoc):
            self.update(myDoc)
            return True
        return False

    def _getCampMap(self, reqDoc):
        """
        Parse the request dictionary and creates a map of output datasets and the
        campaign name they are associated to.
        :param reqDoc: a request dictionary retrieved from ReqMgr2
        :return: a dictionary like
        {"campaign_name_A": ["dataset_A", "dataset_B"],
         "campaign_name_B": ["dataset_C"]}
        """
        campOutputMap = {}
        if reqDoc["RequestType"] in ["StepChain", "TaskChain"] and "ChainParentageMap" in reqDoc:
            for key in reqDoc["ChainParentageMap"]:
                # key is Step1, Step2 or Task1, Task2, etc
                # use the Task/Step level campaign, fallback to the top level one
                if not reqDoc["ChainParentageMap"][key]["ChildDsets"]:
                    # this task/step does not stage any output data
                    continue
                campName = reqDoc[key].get("Campaign", reqDoc.get("Campaign"))
                campOutputMap.setdefault(campName, [])
                campOutputMap[campName].extend(reqDoc["ChainParentageMap"][key]["ChildDsets"])
        else:
            campOutputMap[reqDoc["Campaign"]] = reqDoc["OutputDatasets"]
        return campOutputMap

    def _setOutputMap(self, reqDoc, thisDoc):
        """
        Provided the request content retrieved from ReqMgr2, build the parameters
        associated to every output dataset, including the campaign name.
        :param reqDoc: meant to be the request dictionary retrieved from ReqMgr2
        :param thisDoc: meant to be a template msoutput object
        """
        outputMap = []
        campaignMap = self._getCampMap(reqDoc)
        for camp, dsets in viewitems(campaignMap):
            for outDset in dsets:
                dsetMap = {tuple[0]:tuple[1] for tuple in self.outputMapSchema()}
                dsetMap['Campaign'] = camp
                dsetMap['Dataset'] = outDset
                outputMap.append(dsetMap)

        ### FIXME: need to validate the final output map values
        # finally, update this object
        thisDoc["OutputMap"] = outputMap

    def updateTime(self):
        """
        __updateTime__
        """
        self.setKey('LastUpdate', int(time()))
