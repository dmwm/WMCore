#!/usr/bin/env python
"""
A base handler from which all handlers inherit from.
Handlers are mapped to messages in the component.
"""

from builtins import str, object

import logging


class BaseHandler(object):
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
