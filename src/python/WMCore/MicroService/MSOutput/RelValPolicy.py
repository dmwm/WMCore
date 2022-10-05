#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module to store the RelVal output data placement policy and
make decisions based on that
"""
from __future__ import print_function, division

from copy import deepcopy
import json
import re
from WMCore.MicroService.Tools.Common import getMSLogger
from WMCore.WMException import WMException


class RelValPolicyException(WMException):
    """
    General exception to be raised when a flaw is found in the RelVal
    output data placement policy
    """
    pass


class RelValPolicy():
    """
    This module will contain the RelVal output data placement policy, where
    destinations will be decided according to the dataset datatier and the
    container lifetime will be decided based on the sample type (pre-release
    or not).

    It's supposed to hold a policy driven by dataset datatier, and it's
    data structure looks like:
    [{"datatier": "tier_1", "destinations": ["rse_name_1", "rse_name_2"]},
     {"datatier": "tier_2", "destinations": ["rse_name_2"]},
     {"datatier": "default", "destinations": ["rse_name_3"]}]

    The lifetime policy data structure is something like:
    [{"releaseType": "pre", "lifetimeSecs": 120},
     {"releaseType": "default", "lifetimeSecs": 360}]

    the 'default' key matches the case where a datatier is not specified
    in the policy.
    """

    def __init__(self, tierPolicy, lifetimeDesc, listDatatiers, listRSEs, logger=None):
        """
        Given a policy data structure - as a list of dictionaries - it
        will validate the policy, the datatiers and RSEs defined in it,
        and it will convert the policy into a flat dictionary for easier
        data lookup.
        :param tierPolicy: list of dictionary items with the output rules
        :param lifetimeDesc: list of dictionary items with the output
            lifetime rules
        :param listDatatiers: flat list of existent datatiers in DBS
        :param listRSEs: flat list of existent Disk RSEs in Rucio
        :param logger: logger object, if any
        """
        self.origTierPolicy = deepcopy(tierPolicy)
        self.origLifetimePolicy = deepcopy(lifetimeDesc)

        self.logger = getMSLogger(verbose=False, logger=logger)

        self._validateTierPolicy(tierPolicy, listDatatiers, listRSEs)
        self.tierPolicy = self._convertTierPolicy(tierPolicy)

        self._validateLifetimePolicy(lifetimeDesc)
        self.lifeTPolicy = self._convertLifePolicy(lifetimeDesc)

        # regex to match against CMSSW pre-releases only, e.g.: CMSSW_1_2_3_pre12
        self.preRegex = re.compile(r'CMSSW(_\d+){3}_pre(\d+)$')

    def __str__(self):
        """
        Stringify this object, printing the original policy
        """
        objectOut = dict(originalTierPolicy=self.origTierPolicy, mappedTierPolicy=self.tierPolicy,
                         originalLifetimePolicy=self.origLifetimePolicy, mappedLifetimePolicy=self.lifeTPolicy)
        return json.dumps(objectOut)

    def _validateTierPolicy(self, policyDesc, validDBSTiers, validDiskRSEs):
        """
        This method validates the overall policy data structure, including:
         * internal and external data types
         * whether the datatiers exist in DBS
         * whether the RSEs exist in Rucio
        :param policyDesc: list of dictionaries with the policy definition
        :param validDBSTiers: list with existent DBS datatiers
        :param validDiskRSEs: list with existent Rucio Disk RSEs
        :return: nothing, but it will raise an exception if any validation fails
        """
        if not isinstance(policyDesc, list):
            msg = "The RelVal output data placement policy is not in the expected data type. "
            msg += "Type expected: list, while the current data type is: {}. ".format(type(policyDesc))
            msg += "This critical ERROR must be fixed."
            raise RelValPolicyException(msg) from None

        # policy must have a default/fallback destination for datatiers not explicitly listed
        hasDefault = False
        for item in policyDesc:
            # validate the datatier
            if not isinstance(item['datatier'], str):
                msg = "The 'datatier' parameter must be a string, not {}.".format(type(item['datatier']))
                raise RelValPolicyException(msg) from None
            if item['datatier'] == "default":
                hasDefault = True
            elif item['datatier'] not in validDBSTiers:
                raise RelValPolicyException("Datatier '{}' does not exist in DBS.".format(item['datatier']))

            # validate the destinations
            if not isinstance(item['destinations'], list):
                msg = "The 'destinations' parameter must be a list, not {}".format(type(item['destinations']))
                raise RelValPolicyException(msg) from None
            for rseName in item['destinations']:
                if rseName not in validDiskRSEs:
                    msg = "Destinations '{}' does not exist in Rucio.".format(rseName)
                    raise RelValPolicyException(msg) from None

        if hasDefault is False:
            msg = "A 'default' key must be defined with default destinations."
            raise RelValPolicyException(msg) from None

    def _validateLifetimePolicy(self, lifeTDesc):
        """
        This method validates the lifetime RelVal policy data structure.

        [{"releaseType": "pre", "lifetimeSecs": 120},
         {"releaseType": "default", "lifetimeSecs": 360}]
        :param lifeTDesc: list of dictionaries with the lifetime policy definition
        :return: nothing, but it will raise an exception if any validation fails
        """
        if not isinstance(lifeTDesc, list):
            msg = "The RelVal lifetime output data placement policy is not in the expected data type. "
            msg += "Type expected: list, while the current data type is: {}. ".format(type(lifeTDesc))
            msg += "This critical ERROR must be fixed."
            raise RelValPolicyException(msg) from None

        # policy must have a default/fallback destination for non pre-releases
        setRelTypes = set()
        expRelTypesKeys = {"pre", "default"}
        for item in lifeTDesc:
            if not isinstance(item.get('releaseType', None), str):
                msg = "The 'releaseType' parameter must be a string, not {}.".format(type(item['releaseType']))
                raise RelValPolicyException(msg) from None
            if item['releaseType'] not in expRelTypesKeys:
                msg = "The 'releaseType' parameter does not match the expected values. "
                msg += "Value provided '{}' not in {}.".format(item['releaseType'], expRelTypesKeys)
                raise RelValPolicyException(msg) from None
            if not isinstance(item['lifetimeSecs'], int):
                msg = "The 'lifetimeSecs' parameter must be integer, not {}".format(type(item['lifetimeSecs']))
                raise RelValPolicyException(msg) from None
            if item['lifetimeSecs'] <= 0:
                msg = "The 'lifetimeSecs' parameter cannot be 0 or negative"
                raise RelValPolicyException(msg) from None
            setRelTypes.add(item['releaseType'])

        # last check, it must contain a policy for "pre" releases and "default"
        if setRelTypes != expRelTypesKeys:
            msg = "Policy must define rules for these 2 sample types: {}".format(expRelTypesKeys)
            raise RelValPolicyException(msg) from None

    def _convertTierPolicy(self, policyDesc):
        """
        Maps the RelVal tier data policy to a flat dictionary key'ed by datatiers
        :param policyDesc: list of dictionaries with the tier policy definition
        :return: a dictionary with a map of the RelVal tier policy
        """
        outputPolicy = dict()
        for item in policyDesc:
            outputPolicy.update({item['datatier']: item['destinations']})
        return outputPolicy

    def _convertLifePolicy(self, policyDesc):
        """
        Maps the RelVal lifetime data policy to a flat dictionary key'ed
        by the release cycle type (only supports pre or anything else).
        :param policyDesc: list of dictionaries with the lifetime policy definition
        :return: a dictionary with a map of the RelVal lifetime policy
        """
        outputPolicy = dict()
        for item in policyDesc:
            outputPolicy.update({item['releaseType']: item['lifetimeSecs']})
        return outputPolicy

    def getDestinationByDataset(self, dsetName):
        """
        Provided a dataset name, return the destination defined for its datatier.
        :param dsetName: a string with the full dataset name
        :return: a list of locations
        """
        _, dsn, procString, dataTier = dsetName.split('/')
        return self.tierPolicy.get(dataTier, self.tierPolicy['default'])

    def _isPreRelease(self, dsetName):
        """
        Helper function to determine whether the provided dataset name
        belongs to a pre-release or not.
        :param dsetName: string with the dataset name
        :return: boolean whether it's a pre-release sample or not.
        """
        try:
            procString = dsetName.split('/')[2]
            acqEra = procString.split('-')[0]
        except Exception:
            raise RuntimeError("RelVal dataset name invalid: {}".format(dsetName)) from None
        return bool(self.preRegex.match(acqEra))

    def getLifetimeByDataset(self, dsetName):
        """
        Provided a dataset name, return the rule lifetime defined for
        this sample/release type.
        :param dsetName: a string with the full dataset name
        :return: an integer with the lifetime in seconds
        """
        if self._isPreRelease(dsetName):
            return self.lifeTPolicy["pre"]
        return self.lifeTPolicy["default"]
