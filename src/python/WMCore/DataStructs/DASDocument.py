#!/usr/bin/env python
"""
_DASDocument_

A class representing the structure of data in DAS. The cannonical description of
this structure is found on:
        https://twiki.cern.ch/twiki/bin/view/CMS/DMWMDataAggregationService
"""


class DASDocument(dict):
    def __init__(self, service):
        """
        Make an empty dictionary representing a DAS document
        """
        dict = {"request_timestamp": 0,
                "request_url": "",
                "request_method": "GET",
                "request_params": {},
                "response_version": 0,
                "response_expires": 0,
                "response_checksum": "",
                "request_api": "",
                "call_time": 0}
        self.keys = dict.keys()
        self.setdefault(service, dict)

    def compare(self, dict):
        """
        Is the provided dict a valid DAS document
        """
        pass

    def validate(self):
        """
        Is this instance of a DAS document valid?
        """

        return True
