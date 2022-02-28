#!/usr/bin/env python
#pylint: disable=E1101,C0103,R0902


import unittest

from Utils.PythonVersion import PY3

from WMCore.Configuration import ConfigSection
from WMCore.Configuration import Configuration
from WMCore.Configuration import ConfigurationEx
from WMCore.Configuration import loadConfigurationFile
from WMCore.Configuration import saveConfigurationFile

from WMQuality.TestInit import TestInit


class ConfigurationExTest(unittest.TestCase):
    """
    test case for Configuration object

    """
    def setUp(self):
        """set up"""
        self.testInit = TestInit(__file__)
        self.testDir  = self.testInit.generateWorkDir()
        self.functionSave = "%s/WMCore_Agent_Configuration_t_function.py" % self.testDir


    def tearDown(self):
        """clean up"""
        self.testInit.delWorkDir()


    def testCallableConfigParams(self):
        """ctor"""
        def f():
            return True

        config = Configuration()
        config.section_("SectionF")
        #creating field for the following test
        config.SectionF.aFunction = ''
        #Cannot set a function for plain Configuration objects
        #config.SectionF.__setattr__('aFunction', f)
        self.assertRaises(RuntimeError, config.SectionF.__setattr__, config.SectionF.aFunction, f)

        config = ConfigurationEx()
        config.section_("SectionF")
        #No failures with configurationEx
        config.SectionF.aFunction = f

        #However ConfigurationEx instances cannot be saved
        self.assertRaises(RuntimeError, saveConfigurationFile, config, self.functionSave)


class ConfigurationTest(unittest.TestCase):
    """
    test case for Configuration object

    """
    def setUp(self):
        """set up"""
        self.testInit = TestInit(__file__)
        self.testDir  = self.testInit.generateWorkDir()
        self.normalSave = "%s/WMCore_Agent_Configuration_t_normal.py" % self.testDir
        self.docSave = "%s/WMCore_Agent_Configuration_t_documented.py" % self.testDir
        self.commentSave = "%s/WMCore_Agent_Configuration_t_commented.py" % self.testDir
        if PY3:
            self.assertItemsEqual = self.assertCountEqual

    def tearDown(self):
        """clean up"""
        self.testInit.delWorkDir()


    def testA(self):
        """ctor"""
        try:
            dummyConfig = Configuration()
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

        class DummyObject:
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

        badDict = { "dict" : {}, "list": [DummyObject()], "tuple" : () }
        self.assertRaises(
            RuntimeError, setattr,
            config.Section2, "BadDict", badDict)

        goodDict = { "dict" : {}, "list": [], "tuple" : () }
        config.Section2.GoodDict = goodDict


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
        config.Section1.Parameter5 = {"test1" : "test2", "test3" : 123}

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

        dummyStringSave = str(config)
        dummyDocumentSave = config.documentedString_()
        dummyCommentSave = config.commentedString_()

        saveConfigurationFile(config, self.normalSave)
        saveConfigurationFile(config, self.docSave, document = True)
        saveConfigurationFile(config, self.commentSave, comment = True)

        dummyPlainConfig = loadConfigurationFile(self.normalSave)

        dummyDocConfig = loadConfigurationFile(self.docSave)

        dummyCommentConfig = loadConfigurationFile(self.commentSave)

        #print commentConfig.commentedString_()
        #print docConfig.documentedString_()
        #print docConfig.commentedString_()


    def testF(self):
        """
        Test internal functions pythonise_, listSections_
        """
        config = ConfigSection("config")

        config.section_("SectionA")
        config.section_("SectionB")
        config.SectionA.section_("Section1")
        config.SectionA.section_("Section2")
        config.SectionA.Section1.x   = 100
        config.SectionA.Section1.y   = 100

        pythonise = config.pythonise_()

        assert "config.section_('SectionA')"      in pythonise, "Pythonise failed: Could not find SectionA"
        assert "config.SectionA.Section1.x = 100" in pythonise, "Pythonise failed: Could not find x"

        pythonise = config.SectionA.pythonise_()

        assert "SectionA.section_('Section1')" in pythonise, "Pythonise failed: Could not find Section1"
        assert "SectionA.Section1.x = 100"     in pythonise, "Pythonise failed: Could not find x"

        self.assertItemsEqual(config.listSections_(), ['SectionB', 'SectionA'])
        self.assertItemsEqual(config.SectionA.listSections_(), ['Section2', 'Section1'])


    def testG_testStaticReferenceToConfigurationInstance(self):
        """
        test Configuration.getInstance() which returns reference
        to the Configuration object instance.

        """
        config = Configuration()
        instance = Configuration.getInstance()
        self.assertFalse(hasattr(instance, "testsection"))
        config.section_("testsection")
        self.assertTrue(hasattr(instance, "testsection"))
        config.testsection.var = 10
        self.assertEqual(instance.testsection.var, 10)


    def testH_ConfigSectionDictionariseInternalChildren(self):
        """
        The test checks if any item of the dictionary_whole_tree_()
        result is not unexpanded instance of ConfigSection.

        """
        config = ConfigSection("config")
        config.value1 = "MyValue1"
        config.section_("Task1")
        config.Task1.value2 = "MyValue2"
        config.Task1.section_("subSection")
        config.Task1.subSection.value3 = "MyValue3"
        d = config.dictionary_whole_tree_()
        for values in d.values():
            self.assertFalse(isinstance(values, ConfigSection))
        self.assertEqual(d["Task1"]["subSection"]["value3"], "MyValue3")


    def testI_testGetInternalName(self):
        """
        Test we properly retrieve the internal name of the configuration object

        """
        config = ConfigSection("config")
        name = config.getInternalName()
        self.assertEqual(name, "config")


if __name__ == '__main__':
    unittest.main()
