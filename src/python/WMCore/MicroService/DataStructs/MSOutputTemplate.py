"""
File       : MSOutputTemplate.py
Description: Provides a document Template for MSOutput MicroServices
"""

import time

from copy import deepcopy
from bson import Timestamp


class MSOutputTemplate(dict):
    """
    A simple template to represent the objects stored in Mongodb for MSOutput
    Its purpose should be to serve as a bidirectional representation of the
    objects stored in Mongodb. The instances of this class should be used as a
    buffer for both inbound (reading from the database) and outbound (writing to
    database) documents. And it must have all the methods needed to set or update
    various fields in the object. So that we assure the uniformity of all documents.

    Template format:
    {
    "RequestName": "ReqName",
    "RequestStatus": "(clompletd|cloed-out|announced),
    'Campaign': "Campaign",
    "creationTime": integer timestamp,
    "lastUpdate": integer timestamp,
    "isRelVal": (True|False),
    "isTaken": (True|False),
    "destination": ["list of locations"],
    "OutputDatasets": ["list of datasets"],
    "destinationOutputMap": [{"destination": ["list of locations"],
                              "datasets": ["list of datasets"]},
                             {"destination": ["list of locations"],
                              "datasets": ["list of datasets"]}],
    "campaignOutputMap": [{"campaignName": "blah",
                           "datasets": ["list of datasets"]},
                          {"campaignName": "blah",
                           "datasets": ["list of datasets"]}],
    "transferStatus": "blah", # either "pending" or "done",
    "transferIDs": ["list of transfer IDs"],
    "numberOfCopies": integer
    }
    """
    # TODO:
    #    To add an identifier parameter to __init__
    #    to distinguish inbound from outbound documents.

    # DONE:
    #    to figure out how to pass params as dict not only kwargs so I can use
    #    the same logic for both inbound and outbound documents

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

    def __init__(self, doc=None, **kwargs):
        super(MSOutputTemplate, self).__init__(**kwargs)
        # super(MSOutputTemplate, self).__init__()

        docTemplate = [
            ('_id', None, unicode),
            ('RequestName', None, unicode),
            ('RequestStatus', None, unicode),
            ('Campaign', None, unicode),
            ('creationTime', None, Timestamp),
            ('lastUpdate', None, Timestamp),
            ('isRelVal', None, bool),
            ('isTaken', False, bool),
            ('isTakenby', None, (str, unicode)),
            ('OutputDatasets', None, list),
            ('destination', None, list),
            ('destinationOutputMap', None, dict),
            ('campaignOutputMap', None, dict),
            ('transferStatus', None, unicode),
            ('transferIDs', None, int),
            ('numberOfCopies', None, int)]

        self.docTemplate = docTemplate
        self.required = ['_id', 'RequestName', 'creationTime']
        self.allowEmpty = ['OutputDatasets']

        myDoc = dict()
        for tup in docTemplate:
            myDoc.update({tup[0]: tup[1]})

        # set creation time - if already present in the document it will be
        # overwritten with the value from the document itself during the _checkAttr
        # call and this value here will be ignored
        myDoc['creationTime'] = Timestamp(int(time.time()), 1)

        # if no document was passed consider only **kwargs
        if doc is not None:
            # Search the keys from the document template for their equivalent into
            # the document passed and fill them into internal document so that they
            # can pass the needed checks later, throw the unneeded/unrecognised
            # key/values from the passed document
            for key in [tup[0] for tup in docTemplate]:
                if key in doc.keys():
                    myDoc[key] = deepcopy(doc[key])

        # if any object attribute is passed as a **kwargs parameter then
        # overwrite the equivalent parameter which was coming with the doc
        self._checkAttr(docTemplate, myDoc, update=True, throw=True, **kwargs)

        # enforce a full check on the final document
        self._checkAttr(docTemplate, myDoc, update=False, throw=True, **myDoc)

        # final validation:
        if self._checkValid(myDoc, throw=True):
            self.update(myDoc)

    def checkAttr(self):
        """
        A function to perform a self consistency check
        """
        return self._checkAttr(self.docTemplate, self, update=False, throw=False, **self)

    def _checkAttr(self, docTemplate, myDoc, update=False, throw=False, **kwargs):
        """
        Basically checks everything given in **kwargs against the docTemplate and
        if it passes the check then it is absorbed in myDoc or just the
        result from the check is returned depending on the flags given
        :docTemplate: The document Templaint against which the check is performed
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
        for kw in kwargs.keys():
            found = False
            typeok = False
            for tup in docTemplate:
                if kw == tup[0]:
                    found = True
                    # NOTE: Here we can allow more than one type per field if we
                    #       set them as a tuple of types eg. (str, unicode)
                    if isinstance(kwargs[kw], tup[2]) or kwargs[kw] is None:
                        typeok = True
                        if update:
                            # NOTE: Here we may consider deepcopy
                            myDoc[kw] = kwargs[kw]
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
            if mandField in myDoc.keys() and myDoc[mandField]:
                valid.append(True)
            else:
                valid.append(False)
                missing.append(mandField)

        for mandField in self.allowEmpty:
            if mandField in myDoc.keys():
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
        myDoc = {key: value}
        if self._checkAttr(self.docTemplate, myDoc, throw=True, update=False, **myDoc):
            self.update(myDoc)
            return True
        return False

    def setRelVal(self, isRelVal):
        """
        A method to be used for setting the isRelval key in the document
        """
        myDoc = {'isRelVal': isRelVal}
        if self._checkAttr(self.docTemplate, myDoc, throw=False, update=False, **myDoc):
            self.update(myDoc)
            return True
        return False

    def setWflowType(self):
        """
        __setWflowType__
        """
        pass

    def setCampMap(self):
        """
        __setCampMap__
        """
        pass

    def setDestMap(self):
        """
        __setDestMap_
        """
        pass

    def updateTime(self):
        """
        __updateTeim__
        """
        self.setKey('lastUpdate', Timestamp(int(time.time()), 1))

    def updateStatus(self):
        """
        __updateStatus__
        """
        pass

    def strip(self):
        """
        __strp__
        """
        pass
