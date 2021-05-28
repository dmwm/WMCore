#!/usr/bin/env python
#pylint: disable=E1101,C0103,R0902

from builtins import str, range, object

import unittest
import os
import tempfile
import shutil
from WMCore.Agent.Configuration import Configuration
from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Agent.Configuration import saveConfigurationFile


class ConfigurationTest(unittest.TestCase):
    """
    test case for Configuration object

    """
    def setUp(self):
        """set up"""
        self.tempdir = tempfile.mkdtemp()
        self.normalSave = os.path.join(self.tempdir, "WMCore_Agent_Configuration_t_normal.py")
        self.docSave = os.path.join(self.tempdir, "WMCore_Agent_Configuration_t_documented.py")
        self.commentSave = os.path.join(self.tempdir, "WMCore_Agent_Configuration_t_commented.py")

    def tearDown(self):
        """clean up"""
        shutil.rmtree( self.tempdir )




    def testA(self):
        """ctor"""
        try:
            config = Configuration()
        except Exception as ex:
            msg = "Failed to instantiate Configuration\n"
            msg += str(ex)
            self.fail(msg)


    def testB(self):
        """add settings"""


        config = Configuration()
        config.section_("Section1")

        section1 = getattr(config, "Section1", None)
        self.assertTrue(section1 != None)

        config.section_("Section2")
        section2 = getattr(config, "Section2", None)
        self.assertTrue(section2 != None)

        self.assertRaises(AttributeError, getattr, config, "Section3")

        # basic types
        config.Section1.Parameter1 = True
        config.Section1.Parameter2 = "string"
        config.Section1.Parameter3 = 123
        config.Section1.Parameter4 = 123.456




        self.assertEqual(config.Section1.Parameter1, True)
        self.assertEqual(config.Section1.Parameter2, "string")
        self.assertEqual(config.Section1.Parameter3, 123)
        self.assertEqual(config.Section1.Parameter4, 123.456)

        # dictionary format:
        try:
            section1Dict = config.Section1.dictionary_()
        except Exception as ex:
            msg = "Error converting section to dictionary:\n"
            msg += "%s\n" % str(ex)
            self.fail(msg)

        self.assertTrue( "Parameter1" in section1Dict)
        self.assertTrue( "Parameter2" in section1Dict)
        self.assertTrue( "Parameter3" in section1Dict)
        self.assertTrue( "Parameter4" in section1Dict)

        self.assertEqual(section1Dict['Parameter1'],
                         config.Section1.Parameter1)
        self.assertEqual(section1Dict['Parameter2'],
                         config.Section1.Parameter2)
        self.assertEqual(section1Dict['Parameter3'],
                         config.Section1.Parameter3)
        self.assertEqual(section1Dict['Parameter4'],
                         config.Section1.Parameter4)


        # compound types

        config.Section2.List = ["string", 123, 123.456, False]
        config.Section2.Dictionary = { "string" : "string",
                                       "int" : 123,
                                       "float" : 123.456,
                                       "bool" : False}
        config.Section2.Tuple = ("string", 123, 123.456, False)


        self.assertEqual(config.Section2.List,
                         ["string", 123, 123.456, False])
        self.assertEqual(config.Section2.Tuple,
                         ("string", 123, 123.456, False))

        class DummyObject(object):
            pass
        # unsupported parameter type
        self.assertRaises(
            RuntimeError, setattr,
            config.Section2, "BadObject", DummyObject())
        # unsupported data type in compound type
        badList = [ DummyObject(), DummyObject()]
        self.assertRaises(
            RuntimeError, setattr,
            config.Section2, "BadList", badList)


    def testC(self):
        """add components"""

        config = Configuration()
        config.component_("Component1")
        config.component_("Component2")
        config.component_("Component3")

        comp1 = getattr(config, "Component1", None)
        self.assertTrue(comp1 != None)
        comp2 = getattr(config, "Component2", None)
        self.assertTrue(comp2 != None)


    def testD(self):
        """test documentation"""


        config = Configuration()
        config.section_("Section1")
        config.Section1.Parameter1 = True
        config.Section1.Parameter2 = "string"
        config.Section1.Parameter3 = 123
        config.Section1.Parameter4 = 123.456
        config.Section1.Parameter5 = {
            "test1" : "test2", "test3" : 123
            }


        config.Section1.document_("""This is Section1""")
        config.Section1.document_("""This is Section1.Parameter1""",
                                  "Parameter1")
        config.Section1.document_("""This is Section1.Parameter2""",
                                  "Parameter2")
        config.Section1.document_("""This is Section1.Parameter3\n with multiline comments""",
                                  "Parameter3")


        try:
            config.Section1.documentedString_()
        except Exception as ex:
            msg = "Error calling ConfigSection.documentedString_:\n"
            msg += "%s\n" % str(ex)
            self.fail(msg)
        try:
            config.Section1.commentedString_()
        except Exception as ex:
            msg = "Error calling ConfigSection.commentedString_:\n"
            msg += "%s\n" % str(ex)
            self.fail(msg)

        try:
            config.documentedString_()
        except Exception as ex:
            msg = "Error calling Configuration.documentedString_:\n"
            msg += "%s\n" % str(ex)
            self.fail(msg)
        try:
            config.commentedString_()
        except Exception as ex:
            msg = "Error calling Configuration.commentedString_:\n"
            msg += "%s\n" % str(ex)
            self.fail(msg)





    def testE(self):
        """test save/load """

        testValues = [
            "string", 123, 123.456,
            ["list", 789, 10.1 ],
            { "dict1" : "value", "dict2" : 10.0 }
            ]

        config = Configuration()
        for x in range(0, 5):
            config.section_("Section%s" % x)
            config.component_("Component%s" % x)
            sect = getattr(config, "Section%s" % x)
            comp = getattr(config, "Component%s" % x)
            sect.document_("This is Section%s" % x)
            comp.document_("This is Component%s" % x)

            for i in range(0, 5):
                setattr(comp, "Parameter%s" % i, testValues[i])
                setattr(sect, "Parameter%s" % i, testValues[i])
                comp.document_("This is Parameter%s" % i,
                               "Parameter%s" %i)
                sect.document_("This is Parameter%s" %i,
                               "Parameter%s" %i)

        stringSave = str(config)
        documentSave = config.documentedString_()
        commentSave = config.commentedString_()


        saveConfigurationFile(config, self.normalSave)
        saveConfigurationFile(config, self.docSave, document = True)
        saveConfigurationFile(config, self.commentSave, comment = True)

        plainConfig = loadConfigurationFile(self.normalSave)

        docConfig = loadConfigurationFile(self.docSave)


        commentConfig = loadConfigurationFile(self.commentSave)

        #print commentConfig.commentedString_()
        #print docConfig.documentedString_()
        #print docConfig.commentedString_()


if __name__ == '__main__':
    #config = loadConfigurationFile("/home/evansde/work/cmssrv49/WMCORE/src/python/WMCore/Agent/testConfigs/test2.py")

    #print config.Agent.wibble

    unittest.main()
