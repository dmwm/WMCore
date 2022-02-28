#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
_WMException_t_

General test for WMException, intended to simulate code

- run from a py2 interpreter
- with modernization to strings, such as
  - with `from builtins import str`
  - with `from builtins import bytes`

"""
from __future__ import print_function, division
from builtins import str, bytes

import logging
import unittest
from WMCore.WMException import WMException


class WMExceptionTest(unittest.TestCase):
    """
    A test of a generic exception class
    """
    def setUp(self):
        """
        setup log file output.
        """
        logging.basicConfig(level=logging.NOTSET,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='%s.log' % __file__,
                            filemode='w')

        self.logger = logging.getLogger('WMExceptionTest')

        self.test_data = {
            'key-ascii': 'value-1',       # unicode (ascii): unicode (ascii)
            'key-ascii-b': '√@ʟυℯ-1',     # unicode (ascii): unicode (non-ascii)
            'key-ascii-c': u'√@ʟυℯ-1',    # unicode (ascii): unicode (non-ascii)
            'key-ascii-d': bytes('√@ʟυℯ-1', 'utf-8'),    # unicode (ascii): bytes (of non-ascii)
            'ḱℯƴ-unicode-a': 'ṽ@łυ℮-2',   # unicode (non-ascii): unicode (non-ascii)
            u'ḱℯƴ-unicode-b': 'ṽ@łυ℮-2',  # unicode (non-ascii): unicode (non-ascii)
            'ḱℯƴ-unicode-d': 'value-\x95',  # unicode (of non-ascii): unicode (invalid byte)
            'key-\x95': 'ṽ@łυ℮-2',  # unicode (invalid byte): unicode (non-ascii)
            'key3': 3.14159,

            # The following line breaks py3 with
            #   TypeError: addInfo() keywords must be strings
            #   when using exception.addInfo(**data)
            # 'ḱℯƴ-unicode-c'.encode('utf-8'): 'ṽ@łυ℮-2',  # bytes (of non-ascii): unicode (non-ascii)

            # This would break WMException, but should not happen
            # 'key4': {
            #     b'ḱℯƴ-unicode-c': 'ṽ@łυ℮-2',  # bytes (of unicode): unicode
            # }

            }

    def tearDown(self):
        """
        nothing to tear down
        """

        pass

    def testException(self):
        """
        create an exception and do some tests (only ascii chars)
        """

        exception = WMException("an exception message with nr. 100", 100)
        self.logger.debug("String version of exception: %s", str(exception))
        self.logger.debug("XML version of exception: %s", exception.xml())
        self.logger.debug("Adding data")
        data = {}
        data['key1'] = 'value1'
        data['key2'] = 3.14159
        exception.addInfo(**data)
        self.logger.debug("String version of exception: %s", str(exception))

    def testExceptionUnicode0(self):
        """
        create an exception with non-ascii chars in message and test WMException.addInfo().
        """

        exception = WMException("an exception message with nr. 100 and some non-ascii characters: ₩♏ℭ☺яε", 100)
        exception.addInfo(**self.test_data)
        self.logger.debug("XML version of exception: %s", exception.xml())
        self.logger.debug("String version of exception: %s", str(exception))

    def testExceptionUnicode1(self):
        """
        create an exception with non-ascii chars in message and test WMException constructor
        """

        exception = WMException("an exception message with nr. 100 and some non-ascii characters: ₩♏ℭ☺яε", 100, **self.test_data)
        self.logger.debug("XML version of exception: %s", exception.xml())
        self.logger.debug("String version of exception: %s", str(exception))
        self.logger.debug("exception.__str__(): %s", type(exception.__str__()))  # from py2 interpreter: <class 'future.types.newbytes.newbytes'>
        self.logger.debug("str(exception): %s", type(str(exception)))  # <class 'future.types.newstr.newstr'>

        # The following line breaks python3 with
        #   in __getitem__; return self.data[key]; KeyError: 0
        # self.logger.debug("bytes(exception): %s", type(bytes(exception)))  # <class 'future.types.newbytes.newbytes'>


if __name__ == "__main__":
    unittest.main()
