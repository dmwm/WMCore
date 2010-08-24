#!/usr/bin/env python
"""
_IMProvQuery_

Lightweight XPath like querying tool for extracting information
from an IMProvNode structure

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: IMProvQuery.py,v 1.1 2008/10/08 15:34:14 fvlingen Exp $"


import re
from IMProv.Predicates import parsePredicate
from IMProv.IMProvException import IMProvException

_MatchPredicate = re.compile("^[\S]+\[[\S]+\]$")

class QueryTerm(dict):
    """
    _QueryTerm_

    Container for a term in the query, including predicate if present
    """
    def __init__(self, queryTerm):
        self.setdefault("Term", queryTerm)
        self.setdefault("Name", None)
        self.setdefault("Predicate", None)
        self.setdefault("LastTerm", False)
        self.parse()

        
    def parse(self):
        """
        _parse_

        Examine the queryTerm and extract the name and predicate
        from it and store them in self.
        """
        if _MatchPredicate.match(self['Term']):
            termSplit = self['Term'].split('[', 1)
            predStr = '[%s' % termSplit[1]
            self['Name'] = termSplit[0]
            try:
                self['Predicate'] = parsePredicate(predStr) 
            except IMProvException, ex:
                ex.addInfo(BadTerm = self['Term'])
                raise ex
            return
        else:
            self['Name'] = self['Term']
        return


    def __call__(self, node):
        """
        _operator()_

        evaluate this query term on a node instance, return
        True if the node matches this term, or false if it does not,
        If the predicate returns a value, then this method will
        return True if its LastTerm flag is set to True
        """
        
        if self['Predicate'] == None:
            #  //
            # // No predicate, name based matching only
            #//  match exact name or wildcard
            if (node.name == self['Name']) or (self['Name'] == "*"):
                return True
            return False
        #  //
        # // Evaluate name match and predicate
        #//
        if not ((node.name == self['Name']) or (self['Name'] == "*")):
            #  //
            # // Predicate but no name match
            #//
            return False
        #  //
        # // matching name and predicate => eval predicate
        #//
        result = self['Predicate'](node)
        if result == True:
            return True
        if self['LastTerm'] == True:
            if result != False:
                return True
        return False
    
    def evaluate(self, node):
        """
        _evaluate_

        Evaluate this term on the node, returning the actual
        output value.
        The term already matches, just evaluate the return value
        """
        if self['Predicate'] == None:
            #  //
            # // No predicate => return the node itself as the result
            #//
            return node
        #  //
        # // return output of predicate
        #//
        result = self['Predicate'](node)
        if result == True:
            result = node
        return result
        
class IMProvQuery:
    """
    _IMProvQuery_

    Query execution object that can be initialised with a path
    like query of the form
    /path1/path2[predicate]/path3[predicate']
    to extract either matching nodes or attributes from an IMProvNode
    structure.
    
    Also provides a container for retrieving matching results
    
    """
   
    
    def __init__(self, query):
        self._Query = query
        self._QueryTerms = []
        self._ParseQuery()
        self.results = []
        self.nodeCache = []
        
    def __call__(self, improvNode):
        """
        _operator()_

        Execute the query on the node structure provided, assuming
        improvNode is the top level node (ie it will be treated as
        / in the query
        """
        if self._Query.startswith("/"):
            #  //
            # // Query from Root Node
            #//  add root node to the nodeCache
            self.nodeCache.append(improvNode)
        else:
            #  //
            # // Relative nodes, find all nodes that match the
            #//  first term in the query
            self.findRelativeNodes(improvNode)

        #  //
        # // nodeCache now contains the list of nodes to evaluate
        #//  the query on
        #print self.nodeCache
        #  // 
        # // Now recursively execute the query on each of the nodes
        #//  in the cache to search for matches. Matches will be
        #  //added to the results cache and the final results
        # // are used to evaluate the result values
        #//
        termlist = self._QueryTerms[:]
        for node in self.nodeCache:
            self.processNodeQuery(node, termlist)

    
        lastTerm = self._QueryTerms[-1]
        returnResults = []
        for result in self.results:
            value = lastTerm.evaluate(result)
            if value != None:
                returnResults.append(value)
        return returnResults
    
    


    def findRelativeNodes(self, node):
        """
        process the whole node tree to find all nodes that
        match the first query Term.
        Recursively process all child nodes.
        """
        firstTerm = self._QueryTerms[0]
        if firstTerm(node):
            self.nodeCache.append(node)
        for child in node.children:
            self.findRelativeNodes(child)
        return
    


    def processNodeQuery(self, improvNode, terms):
        """
        _processNodeQuery_

        process the node provided by matching its
        name to the first term.
        If the first term matches, then process each
        of its child nodes with the reduced list of terms
        """
        if len(terms) == 0:
            #  //
            # // No Terms => No matches
            #//
            return
        firstTerm = terms[0]
        if not firstTerm(improvNode):
            #  //
            # // No match between node name and first term
            #//  => No matches on this node
            return
        
        #  //
        # // Have a node match, if there are still terms, then
        #//  reduce the query, by removing this nodes name and
        #  //and then apply it to the children of the matched node
        # // If there are no more terms, then we have a match to the
        #//  end of the query, so keep a reference to it
        newterms = terms[1:]
        if len(newterms) == 0:
            #  //
            # // No terms => match made, keep ref to node in results
            #//
            self.results.append(improvNode)
            return
        for child in improvNode.children:
            #  //
            # // more terms => act recursively on children
            #//
            self.processNodeQuery(child, newterms)
        return
        

        

        


        
    
    def _ParseQuery(self):
        """
        _ParseQuery_

        Chop the query up into a stack of terms

        """
        qlist = self._Query.split('/')
        while "" in qlist:
            qlist.remove("")
        for item in qlist:
            self._QueryTerms.append(QueryTerm(item))
        self._QueryTerms[-1]['LastTerm'] = True
        return


    

