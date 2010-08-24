#!/usr/bin/env python

"""
Template Action for trigger.
"""

__revision__ = "$Id: ActionTemplate.py,v 1.1 2008/09/08 19:38:02 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"


import logging

class ActionTemplate:
    """
    Template action for testing the trigger
    Other action inherit from this or implement its interface.
    """

    def __init__(self):
        """
        Simple constructor
        """
        self.args = {}
        # this parameter is used in the trigger
        # to see if it can pass multiple jobspec ids (bulk)
        # to a handler through an array.
        self.bulk = False

    def handle(self, payload_and_id):
        """
        Simple handler where payload is allways a dictionary of 'id' and 'payload'
        where payload is the object set in when the action was set.
        """
        logging.debug('-->Action Test Action is being invoked for payload(s): '+ \
            str(payload_and_id) )

    def __call__(self, payload_and_id):
        self.handle(payload_and_id)

