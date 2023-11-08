#!/usr/bin/env python
"""
_PSetTweaks_

Record a set of tweakable parameters from a CMSSW Configuration in a CMSSW
independent python structure

"""

from future import standard_library
standard_library.install_aliases()

from builtins import object, map, range
from past.builtins import basestring
from future.utils import viewitems, viewvalues

import importlib
import inspect
import json
import pickle
import sys
from functools import reduce

class PSetHolder(object):
    """
    _PSetHolder_

    Dummy PSet object used to construct the Tweak object to mimic
    the config structure

    """
    def __init__(self, psetName):
        self.psetName_ = psetName
        self.parameters_ = []

#  //
# // Assistant lambda functions
#//
childPSets = lambda x: [ value for value in viewvalues(x.__dict__)
                         if value.__class__.__name__ == "PSetHolder" ]
childParameters = lambda p, x: [ "%s.%s" % (p,i) for i in  x.parameters_ ]

recursiveGetattr = lambda obj, attr: reduce(getattr, attr.split("."), obj)


def parameterIterator(obj):
    """
    _parameterIterator_

    Util to iterate through the parameters in a PSetHolder

    """
    x = None
    for x in childParameters(obj):
        yield getattr(obj, x)

def psetIterator(obj):
    """
    _psetIterator_

    Util to iterate through the child psets in a PSetHolder
    """
    for x in childPSets(obj):
        yield x




class PSetLister(object):
    """
    _PSetLister_

    Operator to decompose the PSet structure into a
    more sequence like get up

    """
    def __init__(self):
        self.psets = []
        self.parameters = {}
        self.queue = []


    def __call__(self, pset):
        """
        _operator(PSetHolder)_

        recursively traverse all parameters in this and all child
        PSets

        """
        self.queue.append(pset.psetName_)
        psetPath = ".".join(self.queue)
        self.psets.append(psetPath)
        params = childParameters(psetPath, pset)
        self.parameters[psetPath] = params
        list(map(self, childPSets(pset)))
        self.queue.pop(-1)


class JSONiser(object):
    """
    _JSONiser_

    Util class to build a json dictionary structure from the PSet tree
    and also recover the pset tree from a json structure

    """
    def __init__(self):
        self.json = {}
        self.queue = []
        self.parameters = {}


    def __call__(self, pset, parent = None):
        """
        _operator(pset)_

        operate on pset and substructure to build a json dictionary

        """
        if parent == None: parent = self.json

        thisPSet = parent.get(pset.psetName_, None)
        if thisPSet == None:
            parent[pset.psetName_] = {}
            thisPSet = parent[pset.psetName_]

        for param in pset.parameters_:
            thisPSet[param] = getattr(pset, param)
        thisPSet['parameters_'] = pset.parameters_

        for child in childPSets(pset):
            self(child, thisPSet)


    def dejson(self, dictionary):
        """
        _dejson_

        Convert the json structure back to PSetHolders

        """
        params = dictionary.get('parameters_', [])
        queue = ".".join(self.queue)
        for param in params:
            self.parameters["%s.%s" % (queue, param)]  = dictionary[param]
        for key, value in viewitems(dictionary):
            if isinstance(value, dict):
                self.queue.append(key)
                self.dejson(dictionary[key])
                self.queue.pop(-1)








class PSetTweak(object):
    """
    _PSetTweak_

    Template object listing the parameters to be edited.
    Also provides serialisation functionality and defines the
    process + tweak operator to apply the tweaks to a process.

    """

    def __init__(self):
        self.process = PSetHolder("process")

    def addParameter(self, attrName, value):
        """
        _addAttribute_

        Add an attribute as process.pset1.pset2.param = value
        Value should be the appropriate python type

        """
        currentPSet = None
        paramList = attrName.split(".")
        for _ in range(0, len(paramList)):
            param = paramList.pop(0)
            if param == "process":
                currentPSet = self.process
            elif len(paramList) > 0:
                if not hasattr(currentPSet, param):
                    setattr(currentPSet, param, PSetHolder(param))
                currentPSet = getattr(currentPSet, param)
            else:
                setattr(currentPSet, param, value)
                currentPSet.parameters_.append(param)


    def getParameter(self, paramName):
        """
        _getParameter_

        Get value of the parameter with the name given of the
        form process.module...

        """
        if not paramName.startswith("process"):
            msg = "Invalid Parameter Name: %s\n" % paramName
            msg += "Parameter must start with process"
            raise RuntimeError(msg)
        return recursiveGetattr(self, paramName)


    def __iter__(self):
        """
        _iterate_

        Loop over all parameters in the tweak, returning the
        parameter name as a . delimited path and the value

        """
        lister = PSetLister()
        lister(self.process)

        for pset in lister.psets:
            for param in lister.parameters[pset]:
                yield param , self.getParameter(param)

    def psets(self):
        """
        _psets_

        Generator function to yield the PSets in the tweak

        """
        lister = PSetLister()
        lister(self.process)

        for pset in lister.psets:
            yield pset






    def __str__(self):
        """string repr for debugging etc"""
        result = ""
        for x,y in self:
            result += "%s = %s\n" % (x, y)
        return result


    def setattrCalls(self, psetPath):
        """
        _setattrCalls_

        Generate setattr call for each parameter in the pset structure
        Used for generating python format

        """
        result = {}
        current = None
        last = None
        psets = psetPath.split(".")
        for _ in range(0, len(psets)):
            pset = psets.pop(0)
            last = current
            if current == None:
                current = pset
            else:
                current += ".%s" % pset
            if last != None:
                result[current] = "setattr(%s, \"%s\", PSetHolder(\"%s\"))" % (
                    last, pset, pset)
        return result



    def pythonise(self):
        """
        _pythonise_

        return this object as python format

        """
        src = inspect.getsourcelines(PSetHolder)
        result = ""
        for line in src[0]:
            result += line

        result += "\n\n"
        result += "# define PSet Structure\n"
        result += "process = PSetHolder(\"process\")\n"
        setattrCalls = {}
        for pset in self.psets():
            setattrCalls.update(self.setattrCalls(pset))
        order = sorted(setattrCalls.keys())
        for call in order:
            if call == "process": continue
            result += "%s\n" % setattrCalls[call]

        result += "# set parameters\n"
        for param, value in self:
            psetName = param.rsplit(".", 1)[0]
            paramName = param.rsplit(".", 1)[1]
            if isinstance(value, basestring):
                value = "\"%s\"" % value
            result += "setattr(%s, \"%s\", %s)\n" % (
                psetName, paramName, value)
            result += "%s.parameters_.append(\"%s\")\n" % (psetName, paramName)

        return result


    def jsonise(self):
        """
        _jsonise_

        return json format of this tweak

        """
        jsoniser = JSONiser()
        jsoniser(self.process)
        result = json.dumps(jsoniser.json)
        return result

    def jsondictionary(self):
        """
        _jsondictionary_

        return the json layout dictionary, rather than stringing it

        """
        jsoniser = JSONiser()
        jsoniser(self.process)
        return jsoniser.json

    def simplejsonise(self):
        """
        _simplejsonise_

        return simple json format of this tweak
        E.g.:
        {"process.maxEvents.input": 1200, "process.source.firstRun": 1, "process.source.firstLuminosityBlock": 59965}

        """
        jsoniser = JSONiser()
        jsoniser.dejson(self.jsondictionary())
        result = json.dumps(jsoniser.parameters)
        return result


    def persist(self, filename, formatting="python"):
        """
        _persist_

        Save this object as either python, json or pickle

        """
        if formatting not in ("python", "json", "pickle", "simplejson"):
            msg = "Unsupported Format: %s" % formatting
            raise RuntimeError(msg)

        if formatting == "python":
            with open(filename, 'w') as handle:
                handle.write(self.pythonise())
        if formatting == "json":
            with open(filename, "w") as handle:
                handle.write(self.jsonise())
        if formatting == "pickle":
            with open(filename, "wb") as handle:
                pickle.dump(self, handle)
        if formatting == "simplejson":
            with open(filename, "w") as handle:
                handle.write(self.simplejsonise())
        return

    def unpersist(self, filename, formatting=None):
        """
        _unpersist_

        Load data from file provided, if format is not specified, guess
        it based on file extension

        """
        if formatting == None:
            fileSuffix = filename.rsplit(".", 1)[1]
            if fileSuffix == "py":
                formatting = "python"
            if fileSuffix == "pkl":
                formatting = "pickle"
            if fileSuffix == "json":
                formatting = "json"

        if formatting not in ("python", "json", "pickle"):
            msg = "Unsupported Format: %s" % formatting
            raise RuntimeError(msg)

        if formatting == "pickle":
            with open(filename, 'rb') as handle:
                unpickle = pickle.load(handle)
            self.process.__dict__.update(unpickle.__dict__)

        if formatting == "python":
            modSpecs = importlib.util.spec_from_file_location('tempTweak', filename)
            modRef = modSpecs.loader.load_module()
            lister = PSetLister()
            lister(modRef.process)
            for pset in lister.psets:
                for param in lister.parameters[pset]:
                    self.addParameter(param , recursiveGetattr(modRef, param))
            del modRef, sys.modules['tempTweak']


        if formatting == "json":
            with open(filename, 'r') as handle:
                jsonContent = handle.read()


            jsoniser = JSONiser()
            jsoniser.dejson(json.loads(jsonContent))

            for param, value in viewitems(jsoniser.parameters):
                self.addParameter(param , value)

    def reset(self):
        """
        _reset_

        Reset pset holder process

        """
        del self.process
        self.process = PSetHolder("process")


def makeTweakFromJSON(jsonDictionary):
    """
    _makeTweakFromJSON_

    Make a tweak instance and populate it from a dictionary JSON
    structure

    """
    jsoniser = JSONiser()
    jsoniser.dejson(jsonDictionary)
    tweak = PSetTweak()
    for param, value in viewitems(jsoniser.parameters):
        tweak.addParameter(param , value)
    return tweak
