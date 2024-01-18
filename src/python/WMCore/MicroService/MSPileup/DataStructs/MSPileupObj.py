"""
File       : MSPileupObj.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: MSPileupObj module provides MSPileup data structure:

{
    "pileupName": string with the pileup dataset (madatory)
    "pileupType": string with a constant value (mandatory)
    "insertTime": int, seconds since epoch in GMT timezone (service-based)
    "lastUpdateTime": int, seconds since epoch in GMT timezone (service-based)
    "expectedRSEs": ["Disk1", "Disk2", etc], a non-empty list of strings with the RSE names (mandatory)
    "currentRSEs": ["Disk1", "Disk3"], a list of strings with the RSE names (service-based)
    "fullReplicas": integer,  # total number of replicas to keep on Disk (optional)
    "campaigns": [ "name", ... ] # list of workflow campaigns using this pileup (optional)
    "containerFraction": real number with the container fraction to be distributed (optional)
    "replicationGrouping": string with a constant value (DATASET or ALL, (optional)
    "activatedOn": int, seconds since epoch in GMT timezone (service-based)
    "deactivatedOn": int, seconds since epoch in GMT timezone (service-based)
    "active": boolean, (mandatory)
    "pileupSize": integer, current size of the pileup in bytes (service-based)
    "customName": string, custom container DID (optional)
    "transition": [{'updateTime': 123, 'containerFraction': 0.5, 'customDID': 'blah', 'DN': 'blah2'}, ...]
                  list of transition records for partial data placement
    "ruleIds: list of strings (rules) used to lock the pileup id (service-based)
}

The data flow should be done via list of objects, e.g.
[<pileup object 1>, <pileup object 2>, ..., <pileup object n>]
"""

# system modules
import json

# WMCore modules
from Utils.Timers import gmtimeSeconds
from WMCore.MicroService.Tools.Common import getMSLogger
from WMCore.Lexicon import dataset


class MSPileupObj():
    """
    MSPileupObj defines MSPileup data stucture
    """
    def __init__(self, pdict, verbose=None, logger=None, validRSEs=None):
        self.logger = getMSLogger(verbose, logger)
        if not validRSEs:
            validRSEs = []
        self.validRSEs = validRSEs

        self.data = {
            'pileupName': pdict['pileupName'],
            'pileupType': pdict['pileupType'],
            'insertTime': pdict.get('insertTime', gmtimeSeconds()),
            'lastUpdateTime': pdict.get('lastUpdateTime', gmtimeSeconds()),
            'expectedRSEs': pdict['expectedRSEs'],
            'currentRSEs': pdict.get('currentRSEs', []),
            'fullReplicas': pdict.get('fullReplicas', 1),
            'campaigns': pdict.get('campaigns', []),
            'containerFraction': pdict.get('containerFraction', 1.0),
            'replicationGrouping': pdict.get('replicationGrouping', 'ALL'),
            'activatedOn': pdict.get('activatedOn', gmtimeSeconds()),
            'deactivatedOn': pdict.get('deactivatedOn', gmtimeSeconds()),
            'active': pdict['active'],
            'pileupSize': pdict.get('pileupSize', 0),
            'customName': pdict.get('customName', ''),
            'transition': pdict.get('transition', []),
            'ruleIds': pdict.get('ruleIds', [])}
        valid, msg = self.validate(self.data)
        if not valid:
            msg = f'MSPileup input is invalid, {msg}'
            raise Exception(msg)

    def __str__(self):
        """
        Return human readable representation of pileup data
        """
        return json.dumps(self.data, indent=4)

    def getPileupData(self):
        """
        Get pileup data
        """
        return self.data

    def validate(self, pdict=None):
        """
        Validate data according to its schema. If data is not provided via
        input pdict parameter, the validate method will validate internal
        data object.

        :param pdict: input data dictionary (optional)
        :return: (boolean status, string message) result of validation
        """
        msg = ""
        if not pdict:
            pdict = self.data
        docSchema = schema()
        if set(pdict) != set(docSchema):
            pkeys = set(pdict.keys())
            skeys = set(docSchema.keys())
            msg = f"provided object {pkeys} keys are not equal to schema keys {skeys}"
            self.logger.error(msg)
            return False, msg
        for key, val in pdict.items():
            if key not in docSchema:
                msg = f"Failed to validate {key}, not found in {docSchema}"
                self.logger.error(msg)
                return False, msg
            _, stype = docSchema[key]  # expected data type for our key
            if not isinstance(val, stype):
                dtype = str(type(val))     # obtained data type of our value
                msg = f"Failed to validate: {key}, expect data-type {stype} got type {dtype}"
                self.logger.error(msg)
                return False, msg
            if key == 'pileupName' or (key == 'customName' and val != ''):
                if key == 'customName':
                    # we may have additional suffix which we should strip off
                    # before validation, see customDID definition in
                    # WMCore/MicroService/MSPileup/MSPileupData.py
                    val = val.split('-V')[:-1][0]
                try:
                    dataset(val)
                except AssertionError:
                    msg = f"{key} value {val} does not match dataset pattern"
                    self.logger.error(msg)
                    return False, msg
            if key == "pileupType" and val not in ['classic', 'premix']:
                msg = f"pileupType value {val} is neither of ['classic', 'premix']"
                self.logger.error(msg)
                return False, msg
            if key == 'replicationGrouping' and val not in ['DATASET', 'ALL']:
                msg = f"replicationGrouping value {val} is neither of ['DATASET', 'ALL']"
                self.logger.error(msg)
                return False, msg
            if key == 'containerFraction' and (val > 1 or val <= 0):
                msg = f"containerFraction value {val} outside (0,1] range"
                self.logger.error(msg)
                return False, msg
            if key == 'transition' and not self.validateTransitionRecord(val, pdict):
                msg = f"transition record {val} is invalid"
                self.logger.error(msg)
                return False, msg
            if key in ('expectedRSEs', 'currentRSEs') and not self.validateRSEs(val):
                msg = f"{key} value {val} is not in validRSEs {self.validRSEs}"
                self.logger.error(msg)
                return False, msg
            if key == 'expectedRSEs' and len(val) == 0:
                msg = 'document require non-empty list of expectedRSEs'
                self.logger.error(msg)
                return False, msg
        return True, msg

    def validateTransitionRecord(self, records, pdoc):
        """
        Validate given transition records

        :param records: transition records
        :param pdoct: pileup document
        :return: boolean
        """
        for record in records:
            keys = ['updateTime', 'customDID', 'DN', 'containerFraction']
            for key in keys:
                if key not in record:
                    self.logger.error("key '%s' is not present in transition record %s", key, record)
                    return False
                val = record[key]
                if key == 'updateTime' and (val < pdoc['insertTime'] or val > gmtimeSeconds()):
                    # the update time should be in range between pileup insert time and now
                    self.logger.error("wrong value '%s' for updateTime in transition record %s", val, record)
                    return False
                if key in ['customDID', 'DN'] and not isinstance(val, str):
                    self.logger.error("wrong data-type of value '%s' for {key} in transition record %s", val, record)
                    return False
                if key == 'containerFraction' and (val > 1 or val <= 0):
                    self.logger.error("wrong value '%s' for containerFraction in transition record %s", val, record)
                    return False
        return True

    def validateRSEs(self, rseList):
        """
        Validate given list of RSEs

        :param rseList: list of RSEs
        :return: boolean
        """
        if rseList == self.validRSEs:
            return True
        for rse in rseList:
            if rse not in self.validRSEs:
                return False
        return True


def schema():
    """
    Return the data schema for a record in MongoDB.
    It's a dictionary where:
    - key is schema attribute name
    - a value is a tuple of (default value, expected data type)

    :return: a dictionary
    """
    doc = {'pileupName': ('', str),
           'pileupType': ('', str),
           'insertTime': (0, int),
           'lastUpdateTime': (0, int),
           'expectedRSEs': ([], list),
           'currentRSEs': ([], list),
           'fullReplicas': (0, int),
           'campaigns': ([], list),
           'containerFraction': (1.0, float),
           'replicationGrouping': ('', str),
           'activatedOn': (0, int),
           'deactivatedOn': (0, int),
           'active': (False, bool),
           'pileupSize': (0, int),
           'customName': ('', str),
           'transition': ([], list),
           'ruleIds': ([], list)}
    return doc
