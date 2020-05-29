"""
File       : MSOutputStreamer.py

Description: MSOutputStreamer.py class is a simple wrapper for the Output data
placement service in WMCore MicroServices.
"""

# futures
from __future__ import division, print_function

import json
# from itertools import islice
# from collections import deque


class MSOutputStreamer(object):
    """
    MSOutputStreamer class
    """
    # NOTE: This module here is just a placeholder for future development
    #       It is placed here so we can make the basic construction of the MSOutput module

    def __init__(self, requestRecords=None, bufferFile=None, stride=1, logger=None):
        """
        Creates a basic streamer object.
        For the purpose of testing this two modes of streaming are defined:
           - From a dictionary buffer
           - From a file buffer
        :requestRecords: - a dictionary with request records
        :bufferFile:     - a buffer file containing the same information as :requestRecords:
        :stride:         - the stride at which to stream - N records at a time
        """

        self.requestRecords = requestRecords
        self.bufferFile = bufferFile
        self.logger = logger
        self.stride = stride

    def __call__(self, *args, **kwargs):
        """
        The call method from the Streamer class
        """
        return self.streamer()

    def streamer(self):
        """
        The default streamer function
        NOTE:
            If provided bufferFile takes precedence
        TODO:
            For implementing the streaming for stride != 1 we should use:
            itertools.islice(iterable, start, stop[, step]) + collections.deque() 
        """
        try:
            with open(self.bufferFile) as fp:
                result = json.load(fp)
            requests = result['result'][0]
        except Exception as ex:
            msg = "Could not open bufferFile: %s. Exception %s" % (self.bufferFile, str(ex))
            msg += "Falling back to dictionary buffer."
            self.logger.info(msg)
            requests = self.requestRecords

        if requests is None:
            requests = {}

        while requests:
            yield requests.popitem()
