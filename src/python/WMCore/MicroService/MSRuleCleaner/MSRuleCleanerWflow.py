"""
File       : MSRuleCleanerWflow.py
Description: Provides a document Template for the MSRuleCleaner MicroServices
"""

# futures
from __future__ import division, print_function
from builtins import object, str, bytes
from future.utils import viewitems

from copy import deepcopy
from Utils.IteratorTools import flattenList


class WfParser(object):
    """
    Workflow description parser class.
    """
    def __init__(self, docSchema):
        """
        The init method for the Workflow parser class.
        :param docSchema: Document template in the form of a list of tuples as follows:
                          [('KeyName', DefaultValue, type),
                           ('KeyName', DefaultValue, type),
                           ...]
                          To be used for identifying the fields to be searched for
                          in the workflow description

        """
        self.extDoc = {}
        for tup in docSchema:
            self.extDoc[tup[0]] = {'keyName': tup[0],
                                   'values': list(),
                                   'default': tup[1],
                                   'type': tup[2]}

    def __call__(self, wfDescr):
        """
        The Call method for the Workflow parser class.
        """
        self._paramFinder(wfDescr)
        self._wfParse()
        return self.extDoc

    def _paramFinder(self, wfObj):
        """
        Private method used to recursively traverse a workflow description
        and search for all the keyNames defined in the extDoc auxiliary data
        structure. If a 'keyName' happens to be present in several nested levels,
        or in several similar objects from the same level (like {'Task1': {},
        'Task2': {} ...), all the values found are accumulated in the respective
        (flat) list at extDoc[keyName]['values'], which is later to be converted
        to the originally expected type for the given field as described in the
        Document Template
        :param wfObj: Dictionary containing the workflow description

        """
        if isinstance(wfObj, (list, set, tuple)):
            for value in wfObj:
                self._paramFinder(value)
        if isinstance(wfObj, dict):
            for key, value in viewitems(wfObj):
                self._paramFinder(value)
            for key in self.extDoc:
                if key in wfObj:
                    self.extDoc[key]['values'].append(deepcopy(wfObj[key]))

    def _wfParse(self):
        """
        Workflow description parser. Given a document template representing all the
        keyNames to be searched and a workflow description to search in recursively,
        returns all the fields that it can find aggregated according to the rules bellow:
            * if the number of found key instances is 0 - sets the default value from
              the template.
            * if the number of found key instances is 1 - sets the so found value from the
              workflow description and converts it back to the form expected and described
              in the template (removes the outermost list used for value aggregation)
            * if the number of found key instances is > 1 - the values are aggregated
              according to the expected types and data structure defined in the
              template as follows:
                * bool: sets it to True if any of the values found was set to True
                * list: chains/flattens all the sub lists into a single list containing
                        all the values found
                * dict: aggregates/flattens all the key-value pairs from all the
                        dictionaries found into one big dictionary
                        WARNING: (if an inner keyName happens to be found in multiple
                                  dictionaries from the aggregated list of dictionaries
                                  it will be overwritten with the values from the last
                                  one to be merged into the finally constructed dictionary)!
                * str:  will be accumulated in a list containing all the values found
                        WARNING: (will change the expected structure of the field from
                                  a single string to a list of strings)!

        :param wfDescr:     Dictionary with the workflow description
        :param docTemplate: Document template in the form of a list of tuples as follows:
                            [('KeyName', DefaultValue, type),
                             ('KeyName', DefaultValue, type),
                             ...]
                            To be used for identifying the fields to be searched for
                            in the workflow description
        """

        # Convert back the so aggregated extDoc to the original structure:
        for keyName, data in viewitems(self.extDoc):
            if len(data['values']) == 0:
                self.extDoc[keyName] = deepcopy(data['default'])
            elif len(data['values']) == 1:
                self.extDoc[keyName] = deepcopy(data['values'][0])
            elif len(data['values']) > 1:
                if data['type'] is bool:
                    self.extDoc[keyName] = any(data['values'])
                elif data['type'] is list:
                    self.extDoc[keyName] = list(set(flattenList(data['values'])))
                    # WARNING: If it happens this list to be constructed out of elements
                    #          which are instances of unhashable types (e.g. dict, list)
                    #          the set() call will produce an ERR, but this is unlikely
                    #          to happen, see [1] - All the fields we fetch from the
                    #          so nested structure of Task/Step Chain dictionary are
                    #          of hashable types.
                    # [1] https://github.com/dmwm/WMCore/blob/ed40d33069bdddcd98ed5b8430d5ca6662e5941f/src/python/WMCore/WMSpec/StdSpecs/StdBase.py#L1189
                elif data['type'] is dict:
                    self.extDoc[keyName] = {}
                    for item in data['values']:
                        self.extDoc[keyName].update(item)
                elif (isinstance(data['type'], tuple) and (bytes in data['type'] or str in data['type'])) or \
                     (data['type'] is bytes or data['type'] is str):
                    data['values'] = list(set(data['values']))
                    if len(data['values']) == 1:
                        self.extDoc[keyName] = deepcopy(data['values'][0])
                    else:
                        self.extDoc[keyName] = deepcopy(data['values'])


class MSRuleCleanerWflow(dict):
    """
    A minimal workflow and transfer information representation to serve the needs
    of the MSRuleCleaner Micro Service.
    """

    def __init__(self, wfDescr, **kwargs):
        super(MSRuleCleanerWflow, self).__init__(**kwargs)

        # Search for all the keys we need from the ReqManager workflow description
        wfParser = WfParser(self.docSchema())
        myDoc = wfParser(wfDescr)

        # Convert some fields to lists explicitly:
        # NOTE: Those are fields defined as strings in the original workflow
        #       representation, but may turn into lists during the recursive
        #       search and we will use them as lists for the rest of the code.
        for key in ['ParentDataset']:
            if not isinstance(myDoc[key], list):
                if myDoc[key] is None:
                    myDoc[key] = []
                else:
                    myDoc[key] = [myDoc[key]]

        self.update(myDoc)

    def docSchema(self):
        """
        Return the data schema for the document.
        It's a tuple where:
        * 1st element: is the key name / attribute in the request
        * 2nd element: is the default value
        * 3rd element: is the expected data type

        Document format:
            {
            "RequestName": "ReqName",
            "RequestType": "Type",
            "SubRequestType": "Type",
            "RequestStatus": "Status",
            "OutputDatasets": [],
            'RulesToClean': {'plineMSTrCont': [],
                             'plineMSTrBlock': [],
                             'plineAgentCont': [],
                             'plineAgentBlock': []},
            'CleanupStatus': {'plineMSTrCont': False,
                              'plineMSTrBlock': False,
                              'plineAgentCont': False,
                              'plineAgentBlock': False},
            "TransferDone": False  # information - returned by the MSOutput REST call.
            "TransferTape": False  # information - fetched by Rucio about tape rules completion
            "TapeRulesStatus": [('36805b823062415c8ee60300b0e60378', 'OK', '/AToZHToLLTTbar_MA-1900_MH-1200_TuneCP5_13TeV-amcatnlo-pythia8/RunIISummer20UL16RECO-106X_mcRun2_asymptotic_v13-v2/AODSIM'),
                                ('5b75fb7503524449b0f304ea0e52f0de', 'STUCK', '/AToZHToLLTTbar_MA-1900_MH-1200_TuneCP5_13TeV-amcatnlo-pythia8/RunIISummer20UL16MiniAODv2-106X_mcRun2_asymptotic_v17-v2/MINIAODSIM')]
            'TargetStatus': 'normal-archived' || 'rejected-achived' || 'aborted-archived',
            'ParentageResolved': Bool,
            'PlineMarkers': None,
            'IsClean': False
            'IsLogDBClean': False,
            'IsArchivalDelayExpired': False,
            'ForceArchive': False,
            'RequestTransition': [],
            'IncludeParents': False
            'InputDataset': None,
            'ParentDataset': []
            }
        :return: a list of tuples
        """
        docTemplate = [
            ('RequestName', None, (bytes, str)),
            ('RequestType', None, (bytes, str)),
            ('SubRequestType', None, (bytes, str)),
            ('RequestStatus', None, (bytes, str)),
            ('OutputDatasets', [], list),
            ('RulesToClean', {}, dict),
            ('CleanupStatus', {}, dict),
            ('TransferDone', False, bool),
            ('TransferTape', False, bool),
            ('TapeRulesStatus', [], list),
            ('TargetStatus', None, (bytes, str)),
            ('ParentageResolved', True, bool),
            ('PlineMarkers', None, list),
            ('IsClean', False, bool),
            ('IsLogDBClean', False, bool),
            ('IsArchivalDelayExpired', False, bool),
            ('ForceArchive', False, bool),
            ('RequestTransition', [], list),
            ('IncludeParents', False, bool),
            ('InputDataset', None, (bytes, str)),
            ('ParentDataset', None, (bytes, str)),
            ('StatusAdvanceExpiredMsg', "", str)]

        # NOTE: ParentageResolved is set by default to True it will be False only if:
        #       - RequestType is StepChain
        #       - The parent workflow is still in a transient status
        #       this should be one of the flags to be used to estimate if
        #       the workflow is good for archival
        return docTemplate
