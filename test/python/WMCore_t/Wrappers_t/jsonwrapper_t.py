#!/usr/bin/env python
"""
Unit tests for json wrapper.
"""

__revision__ = "$Id: jsonwrapper_t.py,v 1.4 2010/01/12 00:22:49 metson Exp $"
__version__ = "$Revision: 1.4 $"

__test = False
try:
    import cjson
    __test = True
except:
    print "No cjson module is found, skip the test"

import unittest
import json
import os
import WMCore.Wrappers.jsonwrapper as json_wrap

class TestWrapper(unittest.TestCase):
    """
    JSON wrapper unit tests.
    """
    def setUp(self):
        """Init parameters"""
        self.record = {"test":1}

    def test_cjson(self):
        """
        Test cjson implementation.
        """
        json_wrap._module = "cjson"
        result = json_wrap.dumps(self.record)
        expect = json.dumps(self.record)
        self.assertEqual(expect, result)

        data   = result

        result = json_wrap.loads(data)
        expect = json.loads(data)
        self.assertEqual(expect, result)
    
    def test_json(self):
        """
        Test default json implementation.
        """
        json_wrap._module = "json"
        result = json_wrap.dumps(self.record)
        expect = json.dumps(self.record)
        self.assertEqual(expect, result)

        data   = result

        result = json_wrap.loads(data)
        expect = json.loads(data)
        self.assertEqual(expect, result)

    def test_string_compare(self):
        """
        Test that cjson and json libraries do the same thing.
        """
        json_wrap._module = "cjson"
        cj_result = json_wrap.dumps(self.record)
        json_wrap._module = "json"
        dj_result = json_wrap.dumps(self.record)
        self.assertEqual(dj_result, cj_result)

        data   = dj_result
        
        json_wrap._module = "cjson"
        cj_result = json_wrap.loads(data)
        json_wrap._module = "json"
        dj_result = json_wrap.loads(data)
        self.assertEqual(dj_result, cj_result)
        
    def test_file_compare(self):
        testfile = '/tmp/jsonwrappertest'
        
        #write self.record to a file via cjson
        f = open(testfile, 'w')
        json_wrap._module = "cjson"
        json_wrap.dump(self.record, f)
        f.close()
        #read the file with the json
        f = open(testfile, 'r')
        json_wrap._module = "json" 
        data = json_wrap.load(f)
        f.close()
        self.assertEqual(data, self.record) 
        #Clean up
        os.remove(testfile)
        
        #write self.record to a file via json
        f = open(testfile, 'w')
        json_wrap.dump(self.record, f)
        f.close()
        #read the file with cjson
        f = open(testfile, 'r')
        json_wrap._module = "cjson" 
        data = json_wrap.load(f)
        f.close()
        self.assertEqual(data, self.record)
        #Clean up
        os.remove(testfile)
        
if __name__ == "__main__":
    if  __test:
        unittest.main()

