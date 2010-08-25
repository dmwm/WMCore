#!/usr/bin/env python
"""
_TreeSort_ 

Sort objects with parent/child relationships

Return array of items with parents before children

Caveats:
  - Only one parent per item
  - Parent may be an individual object or a list/tuple with one item only
  - Items declaring each other as parents are unsorteable
"""

__revision__ = "$Id: TreeSort.py,v 1.3 2009/06/19 12:37:13 swakef Exp $"
__version__ = "$Revision: 1.3 $"

# pylint: disable-msg=W0104,W0622
try:
    set
except NameError:
    from sets import Set as set
# pylint: enable-msg=W0104,W0622

class _SearchOp:
    """
    __SearchOp_

    Search operator to act on a Tree
    """
    def __init__(self, name):
        self.target = name
        self.result = None

    def __call__(self, node):
        if node.name == self.target:
            self.result = node


class _OrderOp:
    """
    __OrderOp_

    Ordering operator to act on a Tree

    """
    def __init__(self):
        self.result = []

    def __call__(self, _Node):
        self.result.append(_Node.data)



class _Node:
    """
    Tree _Node container class for a generic object that allows
    building a tree of objects based on parentage information

    """
    def __init__(self, name, object):
        self.name = name
        self.data = object
        self.children = {}
        self.parents = {}


    def addChild(self, _Node):
        """
        _addChild_

        Add a child to this _Node
        """
        self.children[_Node.name] = _Node
        self.children[_Node.name].parents[self.name] = self


    def traverse(self,operator):
        """
        _traverse_

        Recursive descent through children
        Call operator on self and then pass down
        """
        operator(self)
        for c in self.children.values():
            c.traverse(operator)


    def stringMe(self, indent = 0):
        """
        _stringMe_

        Recursive print util that indents children to aid debugging
        """
        padding = ""
        for x in range(0, indent):
            padding += " "
        msg = "%s_Node : %s\n" % (padding, self.name)
        for c in self.children.values():
            msg += "%s%s" % (padding, c.stringMe(indent+1))
        return msg

class TreeSort:
    """
    _TreeSort_

    Top level Tree object, maintains a list of File_Node roots
    and allows them all to be queries and sorted in a single operation

    """

    def __init__(self, nameGetter, parentGetter, objects):
        """
        Take a list of objects and init the tree
        name/parentGetter are functions such that:
        nameGetter(object) -> object name
        parentGetter(object) -> object parents
        """
        def __safeParentGetter(item):
            """
            Parentage may or may not be in list form - if not 
              convert to tuple for uniformity
            """
            parents = parentGetter(item)
            if hasattr(parents, '__iter__'):
                return parents
            return (parents,)

        self.roots = {}
        self.init(nameGetter, __safeParentGetter, objects)
        
        
    def init(self, nameGetter, parentGetter, inputs):
        """
        Really do the init here
        """
        remainders = []
        allNames = [ nameGetter(x) for x in inputs ]
        allParents = []
        [ allParents.extend(parentGetter(x)) for x in inputs ]
        externalParents = set(allParents).difference(set(allNames))
    
        #  //
        # // firstly we build the tree roots from all the files that
        #//  dont have parents within the list of files we are dealing with
        for f in inputs:
            name = nameGetter(f)
            parents = set(parentGetter(f))
            # strip out external parents and self
            parents = list(parents - externalParents - set((name,)))
            
            if len(parents) == 0:
                # No parents, top of tree
                self.addRoot(nameGetter(f), f)
                continue
            if len(parents) == 1:
                #f['MatchParent'] = parents[0]
                #remainders.append(f)
                node = _Node(name, f) #cant get parents as need external ones removed
                node.parents[parents[0]] = node
                remainders.append(node)
                continue
            if len(parents) > 1:
                # Multiple parents ==> PANIC!
                msg = "Object %s has too many parents for tree sort"
                raise RuntimeError, msg % name
    
        #  //
        # // Now we have pruned out the roots, we process the
        #//  dependencies for each _Node, we do this recursively
        #  //and make sure the list keeps decreasing
        # //
        #//
        recursionCheck = 0
        remainderLen = len(remainders)
        while len(remainders) > 0:
            remainders = self.process(*remainders)
            if len(remainders) == remainderLen:
                recursionCheck += 1
            remainderLen = len(remainders)
    
            if recursionCheck > 10:
                #  //
                # // further reduction may not be possible
                #//  for now, blow an exception
                msg = "Parentage sorting appears to be stuck in a loop"
                raise RuntimeError, msg

    def addRoot(self, name, object):
        """
        _addRoot_

        Add a new Root _Node to the Tree

        """
        self.roots[name] = _Node(name, object)


    def search(self, name):
        """
        _search_

        Recursive search through all root trees for the LFN
        requested, returning the _Node matching that LFN

        """
        for root in self.roots.values():
            searcher = _SearchOp(name)
            root.traverse(searcher)
            if searcher.result != None:
                return searcher.result

        return None


    def sort(self):
        """
        _sort_

        Collapse tree in order based on parentage tree for each root _Node

        """
        sorter = _OrderOp()
        for root in self.roots.values():
            root.traverse(sorter)
        return sorter.result


    def __str__(self):
        """
        format method to aid debugging
        """
        msg = ""
        for root in self.roots.values():
            msg += "%s\n" % root.stringMe()

        return msg

    def process(self, *input):
        """
        _process_
    
        Reduces the input list for each _Node that it
        can add to the tree.
        Any _Nodes that cannot be added in this pass are returned as
        a list of remainders.
    
        """
        remainders = set()
        for r in input:
            for parent in r.parents:
                searchResult = self.search(parent)
                if searchResult != None:
                    searchResult.addChild(r)
                    continue
                else:
                    remainders.add(r)
        return list(remainders)


if __name__ == '__main__':
    items = [ {'Name' : 'Node1', 'Parents' : []},
               {'Name' : 'Node2', 'Parents' : ['Node1']} ] 
    name = lambda x: x['Name']
    parents = lambda x: x['Parents']
    results = TreeSort(name, parents, items).sort()
    assert(results == items)
    
    items = [ {'Name' : 'Node1', 'Parents' : []},
             {'Name' : 'Node3', 'Parents' : ['Node2']},
               {'Name' : 'Node2', 'Parents' : ['Node1']},
                ] 
    answer = [{'Parents': [], 'Name': 'Node1'},
              {'Parents': ['Node1'], 'Name': 'Node2'},
              {'Parents': ['Node2'], 'Name': 'Node3'}]
    results = TreeSort(name, parents, items).sort()
    assert(results == answer)