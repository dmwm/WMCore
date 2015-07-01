#!/usr/bin/env python
# pylint: disable=C0321,C0103
"""
_Configuration_

Module dealing with Configuration file in python format


"""




import os
import imp
import types
import traceback

_SimpleTypes = [
    bool,
    float,
    bytes,
    str,
    int,
    type(None),
    int,
    ]

_ComplexTypes = [
    dict,
    list,
    tuple,
    ]

_SupportedTypes = []
_SupportedTypes.extend(_SimpleTypes)
_SupportedTypes.extend(_ComplexTypes)


def format(value):
    """
    _format_

    format a value as python
    keep parameters simple, trust python...
    """
    if type(value) == bytes:
        value = "\'%s\'" % value
    return str(value)

def formatNative(value):
    """
    _formatNative_

    Like the format function, but allowing passing of ints, floats, etc.
    """

    if type(value) == int:
        return value
    if type(value) == float:
        return value
    if type(value) == list:
        return value
    if type(value) == dict:
        return dict
    else:
        return format(value)


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

    def __eq__(self, other):
        if (isinstance(other, type(self))):
            return (
                (self._internal_documentation == other._internal_documentation) and
                (self._internal_name == other._internal_name) and
                (self._internal_settings == other._internal_settings) and
                (self._internal_docstrings == other._internal_docstrings) and
                (self._internal_children == other._internal_children) and
                (self._internal_parent_ref == other._internal_parent_ref))
        else:
            return (id(self) == id(other))

    def _complexTypeCheck(self, name, value):
        
        if type(value) in _SimpleTypes:
            return
        elif type(value) in _ComplexTypes:
            vallist = value
            if type(value) == dict:
                vallist = value.values()
            for val in vallist:
                self._complexTypeCheck(name, val)
        else:
            msg = "Not supported type in sequence:"
            msg += "%s\n" % type(value)
            msg += "for name: %s and value: %s\n" % (name, value)
            msg += "Added to WMAgent Configuration"
            raise RuntimeError, msg
                

    def __setattr__(self, name, value):
        if name.startswith("_internal_"):
            # skip test for internal setting
            object.__setattr__(self, name, value)
            return

        if isinstance(value, ConfigSection):
            # child ConfigSection
            self._internal_children.add(name)
            self._internal_settings.add(name)
            value._internal_parent_ref = self
            object.__setattr__(self, name, value)
            return

        if type(value) == str:
            value = str(value)
        
        self._complexTypeCheck(name, value)
        
        object.__setattr__(self, name, value)
        self._internal_settings.add(name)
        return

    def __delattr__(self, name):
        if name.startswith("_internal_"):
            # skip test for internal setting
            object.__delattr__(self, name)
            return
        else:
            if name in self._internal_children:
                self._internal_children.remove(name)
            if name in self._internal_settings:
                self._internal_settings.remove(name)
            object.__delattr__(self, name)
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
                if type(currentSetting) != type(settingInstance) and currentSetting != None and settingInstance != None:
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
        if sectionName in self.__dict__:
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
            if attr in self._internal_docstrings:
                if comment:
                    result.append("# %s.%s: %s" % (
                        myName, attr,
                        self._internal_docstrings[attr].replace("\n", "\n# ")
                        ))
            result.append( "%s.%s = %s" % (
                myName,
                attr, format(getattr(self, attr))
                ))

            if attr in self._internal_docstrings:
                if document:
                    result.append(
                        "%s.document_(\"\"\"%s\"\"\", \'%s\')" % (
                        myName,
                        self._internal_docstrings[attr], attr))
        return result


    def dictionary_(self):
        """
        _dictionary_

        Create a dictionary representation of this object.

        This method does not take into account possible ConfigSections
        as attributes of self (i.e. sub-ConfigSections) as the
        dictionary_whole_tree_() method does.
        The reason for this method to stay is that WebTools.Root.py
        depends on a few places to check itself like:
        if isinstance(param_value, ConfigSection) ...

        """
        result = {}
        [ result.__setitem__(x, getattr(self, x))
          for x in self._internal_settings ]
        return result


    def dictionary_whole_tree_(self):
        """
        Create a dictionary representation of this object.

        ConfigSection.dictionary_() method needs to expand possible
        items that are ConfigSection instances (those which appear
        in the _internal_children set).
        Also these sub-ConfigSections have to be made dictionaries
        rather than putting e.g.
        'Task1': <WMCore.Configuration.ConfigSection object at 0x104ccb50>

        """
        result = {}
        for x in self._internal_settings:
            if x in self._internal_children:
                v = getattr(self, x)
                result[x] = v.dictionary_whole_tree_()
                continue
            result.__setitem__(x, getattr(self, x)) # the same as result[x] = value
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

    #Added by mnorman to make our strategy to use configSections viable
    def listSections_(self):
        """
        _listSections_

        Retrieve a list of components from the components
        configuration section

        """
        comps = self._internal_settings
        return list(comps)



class Configuration(object):
    """
    _Configuration_

    Top level configuration object

    """
    def __init__(self):
        object.__init__(self)
        self._internal_components = []
        self._internal_webapps = []
        self._internal_sections = []
        Configuration._instance = self

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

    def __delattr__(self, name):
        if name.startswith("_internal_"):
            # skip test for internal setting
            object.__delattr__(self, name)
            return
        else:
            if name in self._internal_sections:
                self._internal_sections.remove(name)
            if name in self._internal_components:
                self._internal_components.remove(name)
            if name in self._internal_webapps:
                self._internal_webapps.remove(name)
            object.__delattr__(self, name)
            return

    @staticmethod
    def getInstance():
        return getattr(Configuration, "_instance", None)

    def listComponents_(self):
        """
        _listComponents_

        Retrieve a list of components from the components
        configuration section

        """
        comps = self._internal_components
        return comps

    def listWebapps_(self):
        """
        _listWebapps_

        Retrieve a list of webapps from the webapps configuration section.
        """
        return self._internal_webapps

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
        if sectionName in self.__dict__:
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
            compSection.componentDir = None

        return compSection

    def webapp_(self, webappName):
        """
        _webapp_

        Get the config for the named webapp, add it if not present.  This will
        return a ConfigSection with default fields for the webapp added to it.
        """
        webappSection = self.section_(webappName)
        if webappName not in self._internal_webapps:
            self._internal_webapps.append(webappName)
            webappSection.section_("Webtools")
            webappSection.section_("database")
            webappSection.section_("security")

        return webappSection

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
            if sectionName in self._internal_components:
                result += "config.component_(\'%s\')\n" % sectionName
            elif sectionName in self._internal_webapps:
                result += "config.webapp_(\'%s\')\n" % sectionName
            else:
                result += "config.section_(\'%s\')\n" % sectionName


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
    if  not cfgDirName:
        modPath = imp.find_module(cfgBaseName)
    else:
        modPath = imp.find_module(cfgBaseName, [cfgDirName])
    try:
        modRef = imp.load_module(cfgBaseName, modPath[0],
                                 modPath[1], modPath[2])
    except Exception as ex:
        msg = "Unable to load Configuration File:\n"
        msg += "%s\n" % filename
        msg += "Due to error:\n"
        msg += str(ex)
        msg += str(traceback.format_exc())
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
