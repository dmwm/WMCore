#!/usr/bin/env python
"""
A base handler from which all handlers inherit from.
Handlers are mapped to messages in the component.
"""

__revision__ = "$Id: BaseHandler.py,v 1.1 2008/08/26 13:56:32 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"


import logging


class BaseHandler:
    """
    A base handler from which all handlers inherit from.
    Handlers are mapped to messages in the component.
    """


    def __init__(self, component):
        self.component = component

    def __call__(self, event, payload):
        msg = """
Overload this method to handel event: %s and payload %s
        """ % (str(event), str(payload))
        logging.info(msg)

    def __str__(self):
        """

        return: string

        String representation of the status of this class.
        """
        return str(self.__class__.__name__) 


