#!/usr/bin/env python
# encoding: utf-8
"""
Collection_t.py

Created by Dave Evans on 2010-10-04.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
from WMCore.GroupUser.User import makeUser
from WMCore.ACDC.Collection import Collection


class Collection_t(unittest.TestCase):
    """Collection base object test"""
    def testA(self):
        """instantiation"""
        try:
            coll = Collection()
        except Exception as  ex:
            msg = "Unable to instantiate Collection with no args"
            msg += "\n%s" % str(ex)
            self.fail(msg)

        user = makeUser("somegroup", "someuser")
        coll.setOwner(user)
        self.assertEqual(coll.owner, user)


if __name__ == '__main__':
    unittest.main()
