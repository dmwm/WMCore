#!/usr/bin/env python
"""
Unit tests for json wrapper.
"""

import nose
import unittest
import tempfile
import os

try:
    import simplejson as json
except:
    import json

import WMCore.Wrappers.JsonWrapper as json_wrap

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
        try:
            import cjson
        except:
            raise nose.SkipTest

        json_wrap._module = "cjson"
        result = json_wrap.dumps(self.record)
        expect = json.dumps(self.record)
        self.assertEqual(expect, result)

        data   = result

        result = json_wrap.loads(data)
        expect = json.loads(data)
        self.assertEqual(expect, result)

        try:
            json_wrap.loads("bogusbogus")
        except cjson.DecodeError, ex:
            self.assertEqual(ex.args, ("cannot parse JSON description: bogusbogus",))

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

        try:
            json_wrap.loads("bogusbogus")
        except ValueError, ex:
            self.assertEqual(ex.args, ("No JSON object could be decoded: bogusbogus",))

    def test_string_compare(self):
        """
        Test that cjson and json libraries do the same thing.
        """
        try:
            import cjson
        except:
            raise nose.SkipTest

        json_wrap._module = "cjson"
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
        """
        _test_file_compare_

        """
        try:
            import cjson
        except:
            raise nose.SkipTest

        f = tempfile.NamedTemporaryFile()
        f.delete = False

        json_wrap._module = "cjson"
        #write self.record to a file via cjson
        json_wrap._module = "cjson"
        json_wrap.dump(self.record, f)
        f.close()
        #read the file with the json
        f = open(f.name, 'r')
        json_wrap._module = "json"
        data = json_wrap.load(f)
        f.close()
        self.assertEqual(data, self.record)
        #Clean up
        os.remove(f.name)

        #write self.record to a file via json
        f = tempfile.NamedTemporaryFile()
        f.delete = False
        json_wrap.dump(self.record, f)
        f.close()
        #read the file with cjson
        f = open(f.name, 'r')
        json_wrap._module = "cjson"
        data = json_wrap.load(f)
        f.close()
        self.assertEqual(data, self.record)
        #Clean up
        os.remove(f.name)

if __name__ == "__main__":
    unittest.main()
