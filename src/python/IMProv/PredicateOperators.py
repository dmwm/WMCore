#!/usr/bin/env python
"""
_PredicateOperators_

Operator implementation for Predicates

"""
__revision__ = "$Id: PredicateOperators.py,v 1.1 2008/10/08 15:34:15 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"


def registerPredicateOperator(symbol, funcRef):
    """
    _registerPredicateOperator_

    Register a predicate operator with the symbol provided, if the
    symbol already exists, then raise an Error.
    """
    test = PredicateOperators._PredicateMap.get(symbol, None)
    if test != None:
        msg = "Registered Duplicate Predicate Operator: %s" % funcName
        raise RuntimeError, msg
    PredicateOperators._PredicateMap[symbol] = funcRef
    return



def getPredicateOperator(symbol):
    """
    _getPredicateOperator_

    return a reference to the Predicate operator requested by symbol
    """
    return PredicateOperators._PredicateMap.get(symbol, None)


class PredicateOperators:
    """
    _PredicateOperators_

    Namespace used to store predicate operators by symbol
    """
    _PredicateMap = {}
    def __init__(self):
        msg = "PredicateOperators is a namespace that "
        msg += "should not be instantiated"
        raise RuntimeError, msg
    
    getPredicateOperator = staticmethod(getPredicateOperator)
    registerPredicateOperator = staticmethod(registerPredicateOperator)
    




#  //
# // Note, these are initial primitive implementations, we likely
#//  need to add type comparison and casting logic to the operators
#  //based on the types of objects to be compared
# //
#//


def equalsOperator(leftHandSide, rightHandSide):
    """
    _equalsOperator_

    Evaulate leftHandSide == rightHandSide and return
    True or False
    """
    return leftHandSide == rightHandSide


def notEqualsOperator(leftHandSide, rightHandSide):
    """
    _notEqualsOperator_

    Eval leftHandSide != rightHandSide
    
    """
    return leftHandSide != rightHandSide


def greaterThanOperator(leftHandSide, rightHandSide):
    """
    _greaterThanOperator_

    Eval leftHandSide >= rightHandSide
    
    """
    return leftHandSide >= rightHandSide

def lessThanOperator(leftHandSide, rightHandSide):
    """
    _lessThanOperator_

    Eval leftHandSide <= rightHandSide
    
    """
    return leftHandSide <= rightHandSide


def logicalAnd(leftHandSide, rightHandSide):
    """
    _logicalAnd_

    Eval leftHandSide and rightHandSide

    """
    return leftHandSide and rightHandSide


def logicalOr(leftHandSide, rightHandSide):
    """
    _logicalOr_

    Eval leftHandSide or rightHandSide

    """
    return leftHandSide or rightHandSide


PredicateOperators.registerPredicateOperator("==", equalsOperator)
PredicateOperators.registerPredicateOperator("!=", notEqualsOperator)
PredicateOperators.registerPredicateOperator(">=", greaterThanOperator)
PredicateOperators.registerPredicateOperator("<=", lessThanOperator)
PredicateOperators.registerPredicateOperator("&&", logicalAnd)
PredicateOperators.registerPredicateOperator("||", logicalOr)

