"""
FileSink - store alerts into a file.

"""


import logging
import json
from contextlib import contextmanager


class FileSink(object):
    """
    Class handles storing Alert messages (JSONized) into a file.

    File is being appended, there is no wrapping object representation,
    e.g. list, so direct loading of the entire file content into
    JSON is not possible, yet load() is implemented.

    """


    def __init__(self, config):
        self.config = config
        logging.info("Instantiating ...")
        self.encoder = json.encoder.JSONEncoder()
        self.decoder = json.decoder.JSONDecoder()
        logging.info("Initialized.")


    @contextmanager
    def _handleFile(self, mode, fileName = None):
        fn = fileName or self.config.outputfile
        f = open(fn, mode)
        try:
            yield f
        finally:
            f.close()


    def load(self):
        """
        Return a Python list of Alert instances loaded from the file this
        instance of FileSink is configured with.

        """
        r = []
        with self._handleFile('r') as f:
            for line in f:
                obj = self.decoder.decode(line)
                r.append(obj)
        return r


    def send(self, alerts):
        """
        alerts is a list of Alert instances.

        Writes out new line separated json representation of Alert instances.
        The corresponding test tests that new line character is handled well
        if occurs in the payload of the Alert instance.

        """
        with self._handleFile('a') as f:
            for a in alerts:
                s = self.encoder.encode(a)
                f.write("%s\n" % s)
        logging.debug("Stored %s alerts." % len(alerts))
