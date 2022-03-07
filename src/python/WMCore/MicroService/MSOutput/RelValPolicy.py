#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Module to store the RelVal output data placement policy and
make decisions based on that
"""
from __future__ import print_function, division

from copy import deepcopy
import json
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
    destinations will be decided according to the dataset datatier.

    It's supposed to hold a policy driven by dataset datatier, and it's
    data structure looks like:
    [{"datatier": "tier_1", "destinations": ["rse_name_1", "rse_name_2"]},
     {"datatier": "tier_2", "destinations": ["rse_name_2"]},
     {"datatier": "default", "destinations": ["rse_name_3"]}]

    the 'default' key matches the case where a datatier is not specified
    in the policy.
    """

    def __init__(self, policyDesc, listDatatiers, listRSEs, logger=None):
        """
        Given a policy data structure - as a list of dictionaries - it
        will validate the policy, the datatiers and RSEs defined in it,
        and it will convert the policy into a flat dictionary for easier
        data lookup.
        :param policyDesc: list of dictionary items with the output rules
        :param listDatatiers: flat list of existent datatiers in DBS
        :param listRSEs: flat list of existent Disk RSEs in Rucio
        :param logger: logger object, if any
        """
        self.origPolicy = deepcopy(policyDesc)

        self.logger = getMSLogger(verbose=False, logger=logger)

        self._validatePolicy(policyDesc, listDatatiers, listRSEs)
        self.dictPolicy = self._convertPolicy(policyDesc)

    def __str__(self):
        """
        Stringify this object, printing the original policy
        """
        objectOut = dict(originalPolicy=self.origPolicy, mappedPolicy=self.dictPolicy)
        return json.dumps(objectOut)

    def _validatePolicy(self, policyDesc, validDBSTiers, validDiskRSEs):
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

    def _convertPolicy(self, policyDesc):
        """
        Maps the RelVal data policy to a flat dictionary key'ed by datatiers
        :param policyDesc: list of dictionaries with the policy definition
        :return: a dictionary with a map of the RelVal policy
        """
        outputPolicy = dict()
        for item in policyDesc:
            outputPolicy.update({item['datatier']: item['destinations']})
        return outputPolicy

    def getDestinationByDataset(self, dsetName):
        """
        Provided a dataset name, return the destination defined for its datatier.
        :param dsetName: a string with the full dataset name
        :return: a list of locations
        """
        _, dsn, procString, dataTier = dsetName.split('/')
        return self.dictPolicy.get(dataTier, self.dictPolicy['default'])
