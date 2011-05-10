#!/usr/bin/env python
"""
Unittests for PSetTweaks module

"""
import unittest
import PSetTweaks.PSetTweak as PSetTweak

from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit

class PSetTweakTest(unittest.TestCase):
    """
    unittest for PSetTweak class

    """

    def setUp(self):
        """
        setUp some basic functions

        """
        self.testInit = TestInit(__file__)
        self.testDir  = self.testInit.generateWorkDir()
        return

    def tearDown(self):
        """
        tearDown the test framework

        """
        self.testInit.tearDownCouch()

    def testA(self):
        """init"""

        tweak = PSetTweak.PSetTweak()


    def testB(self):
        """adding attributes"""

        tweak = PSetTweak.PSetTweak()
        tweak.addParameter("process.source.logicalFileNames", ["/store/whatever"])
        tweak.addParameter("process.source.fileNames", ["file:/store/whatever"])


    def testC(self):
        """iterating"""
        tweak = PSetTweak.PSetTweak()
        tweak.addParameter("process.source.logicalFileNames", ["/store/whatever"])
        tweak.addParameter("process.source.fileNames", ["file:/store/whatever"])

        tweak.addParameter("process.module1.module2.module3.parameter1", ["file:/store/whatever", "file:/store/whatever2"])
        tweak.addParameter("process.module1.module2.module3.parameter2", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter3", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter4", 1234)


    def testD(self):
        """pythonise"""
        tweak = PSetTweak.PSetTweak()
        tweak.addParameter("process.source.logicalFileNames", ["/store/whatever"])
        tweak.addParameter("process.source.fileNames", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter1", ["file:/store/whatever", "file:/store/whatever2"])
        tweak.addParameter("process.module1.module2.module3.parameter2", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter3", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter4", 1234)

        #print tweak.pythonise()

        persistFile = "%s/PSetTweak_persist.py" % self.testDir
        tweak.persist(persistFile, "python")

        tweak2 = PSetTweak.PSetTweak()
        tweak2.unpersist(persistFile)

        #print tweak2.pythonise()



    def testE(self):
        """jsonise"""
        tweak = PSetTweak.PSetTweak()
        tweak.addParameter("process.source.logicalFileNames", ["/store/whatever"])
        tweak.addParameter("process.source.fileNames", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter1", ["file:/store/whatever", "file:/store/whatever2"])
        tweak.addParameter("process.module1.module2.module3.parameter2", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter3", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter4", 1234)

        #print tweak.jsonise()


        persistFile = "%s/PSetTweak_persist.json" % self.testDir
        tweak.persist(persistFile, "json")
        tweak2 = PSetTweak.PSetTweak()
        tweak2.unpersist(persistFile)

        #print tweak2.jsonise()


if __name__ == '__main__':
    unittest.main()
