#!/usr/bin/env python
# encoding: utf-8
"""
AutomaticSeeding_t.py

Created by Dave Evans on 2010-08-30.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest


from WMCore.JobSplitting.Generators.AutomaticSeeding import AutomaticSeeding
from WMCore.DataStructs.Job import Job

class AutomaticSeeding_tTests(unittest.TestCase):
    
    
    def testA(self):
        """test creating the plugin"""
        try:
            seeder = AutomaticSeeding()
        except Exception, ex:
            msg = "Failed to instantiate an AutomaticSeeder: "
            msg += str(ex)
            self.fail(msg)
        
        
        
    def testB(self):
        """test plugin acts on a Job as expected"""
        
        job = Job("TestJob")
        seeder = AutomaticSeeding()
        seeder(job)
        


if __name__ == '__main__':
    unittest.main()