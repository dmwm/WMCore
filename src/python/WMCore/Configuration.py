#!/usr/bin/env python
# pylint: disable-msg=C0321,C0103
"""
_Configuration_

Module dealing with Configuration file in python format


"""

__revision__ = "$Id: Configuration.py,v 1.4 2009/02/04 15:26:41 evansde Exp $"
__version__ = "$Revision: 1.4 $"

import os
import imp
import types

_SimpleTypes = [
    types.BooleanType,
    types.FloatType,
    types.StringType,
    types.LongType,
    types.NoneType,
    types.IntType,
    ]

_SupportedTypes = [
    types.DictType,
    types.ListType,
    types.TupleType,
    ]

_SupportedTypes.extend(_SimpleTypes)


def format(value):
    """
    _format_

    format a value as python
    keep parameters simple, trust python...
    """
    if type(value) == types.StringType:
        value = "\'%s\'" % value
    return str(value)

class ConfigSection(object):
    """
    _ConfigSection_

    Chunk of configuration information

    """
    def __init__(self, name = None):
        object.__init__(self)
        self._internal_documentation = ""
        self._internal_name = name
        self._internal_settings = set()
        self._internal_docstrings = {}
        self._internal_children = set()
        self._internal_parent_ref = None

    def __setattr__(self, name, value):
        if name.startswith("_internal_"):
            # skip test for internal settinsg
            object.__setattr__(self, name, value)
            return
        if isinstance(value, ConfigSection):
            # child ConfigSection
            self._internal_children.add(name)
            self._internal_settings.add(name)
            value._internal_parent_ref = self
            object.__setattr__(self, name, value)
            return

        if type(value) not in _SupportedTypes:
            msg = "Unsupported Type: %s\n" % type(value)
            msg += "Added to WMAgent Configuration"
            raise RuntimeError, msg
        if type(value) in (types.ListType, types.TupleType, types.DictType):
            vallist = value
            if type(value) == types.DictType:
                vallist = value.values()
            for val in vallist:
                if type(val) not in _SimpleTypes:
                    msg = "Complex Value type in sequence:"
                    msg += "%s\n" % type(val)
                    msg += "Added to WMAgent Configuration"
                    raise RuntimeError, msg
        object.__setattr__(self, name, value)
        self._internal_settings.add(name)
        return

    def __iter__(self):
        for attr in self._internal_settings:
            yield getattr(self, attr)


    def __add__(self, otherSection):
        """
        _addition operator_

        Define addition for two config section objects

        """
        for setting in otherSection._internal_settings:
            settingInstance = getattr(otherSection, setting)
            if setting in self._internal_settings:
                currentSetting = getattr(self, setting)
                if type(currentSetting) != type(settingInstance):
                    msg = "Trying to overwrite a setting with mismatched types"
                    msg += "%s.%s is not the same type as %s.%s" % (
                        self._internal_name, setting,
                        otherSection._internal_name, setting
                        )


                    raise TypeError, msg
            self.__setattr__(setting, settingInstance)
        return self

    def section_(self, sectionName):
        """
        _section_

        Get a section by name, create it if not present,
        returns a ConfigSection instance

        """
        if self.__dict__.has_key(sectionName):
            return self.__dict__[sectionName]
        newSection = ConfigSection(sectionName)
        self.__setattr__(sectionName, newSection)
        return object.__getattribute__(self, sectionName)



    def pythonise_(self, **options):
        """
        convert self into list of python format strings

        options available

        document - if True will add document_ calls to the python
        comment  - if True will add docs as comments.

        """
        document = options.get('document', False)
        comment  = options.get('comment',  False)
        prefix   = options.get('prefix',   None)

        if prefix != None:
            myName = "%s.%s" % (prefix, self._internal_name)
        else:
            myName = self._internal_name

        result = []
        if document:
            result.append("%s.document_(\"\"\"%s\"\"\")" % (
                myName,
                self._internal_documentation)
                          )
        if comment:
            result.append("# %s: %s" % (
                myName, self._internal_documentation.replace(
                "\n", "\n# "),
                ))
        for attr in self._internal_settings:
            if attr in self._internal_children:
                result.append("%s.section_(\'%s\')" % (myName, attr))
                result.extend(getattr(self, attr).pythonise_(
                    document = document, comment = comment, prefix = myName))
                continue
            if self._internal_docstrings.has_key(attr):
                if comment:
                    result.append("# %s.%s: %s" % (
                        myName, attr,
                        self._internal_docstrings[attr].replace("\n", "\n# ")
                        ))
            result.append( "%s.%s = %s" % (
                myName,
                attr, format(getattr(self, attr))
                ))

            if self._internal_docstrings.has_key(attr):
                if document:
                    result.append(
                        "%s.document_(\"\"\"%s\"\"\", \'%s\')" % (
                        myName,
                        self._internal_docstrings[attr], attr))
        return result

    def dictionary_(self):
        """
        _dictionary_

        Create a dictionary representation of this object

        """
        result = {}
        [ result.__setitem__(x, getattr(self, x))
          for x in self._internal_settings ]
        return result

    def document_(self, docstring, parameter = None):
        """
        _document_

        Add docs/comments to parameters. If the parameter is None, then
        the documentation is provided to the section itself.
        This method will overwrite any existing documentation

        """
        if parameter == None:
            self._internal_documentation = str(docstring)
            return
        self._internal_docstrings[parameter] = str(docstring)
        return


    def __str__(self):
        """
        string representation, dump to python format
        """
        result = ""
        for pystring in self.pythonise_():
            result += "%s\n" % pystring
        return result

    def documentedString_(self):
        """
        string representation, dump to python format
        include docs as calls to document_
        """
        result = ""
        for pystring in self.pythonise_(document = True):
            result += "%s\n" % pystring
        return result

    def commentedString_(self):
        """
        string representation, dump to python format
        include docs as comments
        """
        result = ""
        for pystring in self.pythonise_(comment = True):
            result += "%s\n" % pystring
        return result


class Configuration(object):
    """
    _Configuration_

    Top level configuration object

    """
    def __init__(self):
        object.__init__(self)
        self._internal_components = []
        self._internal_sections = []

    def __add__(self, otherConfig):
        """
        _addition operator_

        Define addition for two config section objects

        """
        for configSect in otherConfig._internal_sections:
            if configSect not in self._internal_sections:
                self.section_(configSect)
            getattr(self, configSect) + getattr(otherConfig, configSect)
        return self



    def __setattr__(self, name, value):
        if name.startswith("_internal_"):
            # skip test for internal settinsg
            object.__setattr__(self, name, value)
            return
        if not isinstance(value, ConfigSection):
            msg = "Can only add objects of type ConfigSection to Configuration"
            raise RuntimeError, msg

        object.__setattr__(self, name, value)
        return


    def listComponents_(self):
        """
        _listComponents_

        Retrieve a list of components from the components
        configuration section

        """
        comps = self._internal_components
        return comps

    def listSections_(self):
        """
        _listSections_

        Retrieve a list of components from the components
        configuration section

        """
        comps = self._internal_sections
        return comps

    def section_(self, sectionName):
        """
        _section_

        Get a section by name, create it if not present,
        returns a ConfigSection instance

        """
        if self.__dict__.has_key(sectionName):
            return self.__dict__[sectionName]
        newSection = ConfigSection(sectionName)
        self.__setattr__(sectionName, newSection)
        self._internal_sections.append(sectionName)
        return object.__getattribute__(self, sectionName)

    def component_(self, componentName):
        """
        _component_

        Get the config for the named component, add it
        if not present, returns a ConfigSection with
        default fields for the component added to it

        """
        compSection = self.section_(componentName)
        if componentName not in self._internal_components:
            self._internal_components.append(componentName)

        compSection.ComponentDir = None
        return compSection




    def pythonise_(self, **options):
        """
        write as python format


        document - if True will add document_ calls to the python
        comment  - if True will add docs as comments.

        """
        document = options.get('document', False)
        comment  = options.get('comment',  False)

        result = "from WMCore.Configuration import Configuration\n"
        result += "config = Configuration()\n"
        for sectionName in self._internal_sections:
            if sectionName not in self._internal_components:
                result += "config.section_(\'%s\')\n" % sectionName
            else:
                result += "config.component_(\'%s\')\n" % sectionName

            sectionRef = getattr(self, sectionName)
            for sectionAttr in sectionRef.pythonise_(
                document = document, comment = comment):
                if sectionAttr.startswith("#"):
                    result += "%s\n" % sectionAttr
                else:
                    result += "config.%s\n" % sectionAttr

        return result

    def __str__(self):
        """
        string format of this object

        """
        return self.pythonise_()


    def documentedString_(self):
        """
        python format with document_ calls
        """
        return self.pythonise_(document = True)


    def commentedString_(self):
        """
        python format with docs as comments
        """
        return self.pythonise_(comment = True)



def loadConfigurationFile(filename):
    """
    _loadConfigurationFile_

    Load a Configuration File

    """

    cfgBaseName = os.path.basename(filename).replace(".py", "")
    cfgDirName = os.path.dirname(filename)
    modPath = imp.find_module(cfgBaseName, [cfgDirName])
    try:
        modRef = imp.load_module(cfgBaseName, modPath[0],
                                 modPath[1], modPath[2])
    except Exception, ex:
        msg = "Unable to load Configuration File:\n"
        msg += "%s\n" % filename
        msg += "Due to error:\n"
        msg += str(ex)
        raise RuntimeError, msg


    for attr in modRef.__dict__.values():

        if isinstance(attr, Configuration):
            return attr

    #  //
    # //  couldnt find a Configuration instance
    #//
    msg = "Unable to find a Configuration object instance in file:\n"
    msg += "%s\n" % filename
    raise RuntimeError, msg




def saveConfigurationFile(configInstance, filename, **options):
    """
    _saveConfigurationFile_

    Save the configuration as a python module
    Options controls the format of documentation

    comment = True means save docs as comments
    document = True means save docs as document_ calls


    """
    comment = options.get("comment", False)
    document = options.get("document", False)
    if document: comment = False

    handle = open(filename, 'w')
    if document:
        handle.write(configInstance.documentedString_())
        return
    elif comment:
        handle.write(configInstance.commentedString_())
    else:
        handle.write(str(configInstance))

    handle.close()
    return
