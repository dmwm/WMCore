#!/usr/bin/env python

"""
Template Action for trigger.
"""






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

    def handle(self, payloadAndId):
        """
        Simple handler where payload is allways a dictionary of 'id' and 'payload'
        where payload is the object set in when the action was set.
        """
        msg = '-->Action Test Action is being invoked for payload(s): '+ \
            str(payloadAndId) 
        logging.debug(msg)

    def __call__(self, payloadAndId):
        self.handle(payloadAndId)

