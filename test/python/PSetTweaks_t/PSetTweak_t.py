#!/usr/bin/env python
"""
Unittests for PSetTweaks module

"""
import unittest
import PSetTweaks.PSetTweak as PSetTweak


class PSetTweakTest(unittest.TestCase):
    """
    unittest for PSetTweak class

    """

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




        for x,y in tweak:
            print x,y


    def testD(self):
        """pythonise"""
        tweak = PSetTweak.PSetTweak()
        tweak.addParameter("process.source.logicalFileNames", ["/store/whatever"])
        tweak.addParameter("process.source.fileNames", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter1", ["file:/store/whatever", "file:/store/whatever2"])
        tweak.addParameter("process.module1.module2.module3.parameter2", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter3", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter4", 1234)

        print tweak.pythonise()

        persistFile = "/tmp/PSetTweak_persist.py"
        tweak.persist(persistFile, "python")

        tweak2 = PSetTweak.PSetTweak()
        tweak2.unpersist(persistFile)

        print tweak2.pythonise()



    def testE(self):
        """jsonise"""
        tweak = PSetTweak.PSetTweak()
        tweak.addParameter("process.source.logicalFileNames", ["/store/whatever"])
        tweak.addParameter("process.source.fileNames", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter1", ["file:/store/whatever", "file:/store/whatever2"])
        tweak.addParameter("process.module1.module2.module3.parameter2", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter3", ["file:/store/whatever"])
        tweak.addParameter("process.module1.module2.module3.parameter4", 1234)

        print tweak.jsonise()


        persistFile = "/tmp/PSetTweak_persist.json"
        tweak.persist(persistFile, "json")
        tweak2 = PSetTweak.PSetTweak()
        tweak2.unpersist(persistFile)

        print tweak2.jsonise()


if __name__ == '__main__':
    unittest.main()
