#!/bin/env python

"""
_ParseXMLFile_

This holds the methods used to take an xmlFilename and return a tree structure.
Used for the expat xml parsers

"""

from builtins import next, str, object
from future.utils import viewitems

import xml.parsers.expat

class Node(object):
    """
    _Node_

    Really simple DOM like container to simplify parsing the XML file
    and formatting the character data without all the whitespace guff

    """
    def __init__(self, name, attrs):
        self.name = str(name)
        self.attrs = {}
        self.text = None
        for k, v in viewitems(attrs):
            self.attrs.__setitem__(str(k), str(v))
        self.children = []

    def __str__(self):

        result = " %s %s \"%s\"\n" % (self.name, self.attrs, self.text)
        for child in self.children:
            result += str(child)
        return result



def coroutine(func):
    """
    _coroutine_

    Decorator method used to prime coroutines

    """
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        next(cr)
        return cr
    return start


def xmlFileToNode(reportFile):
    """
    _xmlFileToNode_

    Use expat and the build coroutine to parse the XML file and build
    a node structure

    """
    node = Node("JobReports", {})
    expat_parse(open(reportFile, 'rb'),
                build(node))
    return node


def expat_parse(f, target):
    """
    _expat_parse_

    Expat based XML parsing that feeds a node building coroutine

    """
    parser = xml.parsers.expat.ParserCreate()
    #parser.buffer_size = 65536
    parser.buffer_text = True

    # a leftover from the py2py3 migration - TO BE REMOVED
    # parser.returns_unicode = False
    parser.StartElementHandler = \
       lambda name,attrs: target.send(('start',(name,attrs)))
    parser.EndElementHandler = \
       lambda name: target.send(('end',name))
    parser.CharacterDataHandler = \
       lambda data: target.send(('text',data))
    parser.ParseFile(f)


@coroutine
def build(topNode):
    """
    _build_

    Node structure builder that is fed from the expat_parse method

    """
    nodeStack = [topNode]
    charCache = []
    while True:
        event, value = (yield)
        if event == "start":
            charCache = []
            newnode = Node(value[0], value[1])
            nodeStack[-1].children.append(newnode)
            nodeStack.append(newnode)

        elif event == "text":
            charCache.append(value)

        else: # end
            nodeStack[-1].text = str(''.join(charCache)).strip()
            nodeStack.pop()
            charCache = []
