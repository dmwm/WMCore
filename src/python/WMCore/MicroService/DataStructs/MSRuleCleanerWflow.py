"""
File       : MSRuleCleanerWflow.py
Description: Provides a document Template for the MSRuleCleaner MicroServices
"""

# futures
from __future__ import division, print_function

from copy import deepcopy


class MSRuleCleanerWflow(dict):
    """
    A minimal workflow and transfer information representation to serve the needs
    of the MSRuleCleaner Micro Service.
    """

    def __init__(self, doc, **kwargs):
        super(MSRuleCleanerWflow, self).__init__(**kwargs)

        # Search for all the keys we need from the ReqManager workflow description
        myDoc = {}
        for tup in self.docSchema():
            if tup[0] in doc:
                myDoc[tup[0]] = deepcopy(doc[tup[0]])
            else:
                myDoc.update({tup[0]: tup[1]})
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
            'TargetStatus': 'normal-archived' || 'rejected-achived' || 'aborted-archived',
            'ParentageResolved': Bool,
            'PlineMarkers': None,
            'IsClean': False
            'ForceArchive', False]
            }
        :return: a list of tuples
        """
        docTemplate = [
            ('RequestName', None, (str, unicode)),
            ('RequestType', None, (str, unicode)),
            ('RequestStatus', None, (str, unicode)),
            ('OutputDatasets', [], list),
            ('RulesToClean', {}, dict),
            ('CleanupStatus', {}, dict),
            ('TransferDone', False, bool),
            ('TargetStatus', None, (str, unicode)),
            ('ParentageResolved', True, bool),
            ('PlineMarkers', None, list),
            ('IsClean', False, bool),
            ('IsLogDBClean', False, bool),
            ('IsArchivalDelayExpired', False, bool),
            ('ForceArchive', False, bool),
            ('RequestTransition', [], list)]

        # NOTE: ParentageResolved is set by default to True it will be False only if:
        #       - RequestType is StepChain
        #       - The parent workflow is still in a transient status
        #       this should be one of the flags to be used to estimate if
        #       the workflow is good for archival
        return docTemplate
