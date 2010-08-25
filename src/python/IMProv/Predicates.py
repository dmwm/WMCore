#!/usr/bin/env python
"""
_Predicates_

Read a Predicate Expression and convert it into
a set of glyphs that can be used to produce
a predicate object tree structure that can be evaluated on a
node

"""




import shlex
import re
import StringIO

from IMProv.IMProvException import IMProvException
from IMProv.PredicateFunctions import PredicateFunctions
from IMProv.PredicateOperators import PredicateOperators

    
class ExpressionParser:
    """
    _ExpressionParser_

    Object that constructs a stack of glyphs from
    a predicate expression string
    
    """
    def __init__(self, expr):
        self.expr = expr
        self.parser = shlex.shlex(StringIO.StringIO(self.expr))
        self.glyphs = []

        self._LastToken = None
        self._Handlers = {
            '=' : self.constructOperator,
            '!' : self.constructOperator,
            '>' : self.constructOperator,
            '<' : self.constructOperator,
            '&' : self.constructOperator,
            '|' : self.constructOperator,
            }
        
        
    def parse(self):
        """
        _parse_

        Parse the expression and convert it into a stack of
        glyphs
        """
        nextToken = self.parser.get_token() 
        while nextToken != "":
            self.handleToken(nextToken)
            self._LastToken = nextToken
            nextToken =  self.parser.get_token() 
            

    def handleToken(self, token):
        """
        _handleToken_

        Handle a token from the parser and use it to create
        a glyph
        """
        if token in self._Handlers.keys():
            self._Handlers[token](token)
            return
        else:
            self.appendToken(token)
            
        
    def constructOperator(self, token):
        """
        _constructOperator_

        All operators are treated as double special characters like
        ==, != , >=, && || etc so this method builds those pairs
        from the single char tokens.
        Validation of the operator combination is done at a later step
        """
        if self._LastToken in ('=', '!', '>', '<', '&', '|'):
            operator = "%s%s" % (self._LastToken, token)
            self._LastToken = None
            self.glyphs[-1] = operator
        else:
            self.appendToken(token)
            

    def appendToken(self, token):
        """
        _appendToken_

        When there is nothing to be done with a token, ie it is its
        own complete entity, add it to the stack
        """
        self.glyphs.append(token)
        
class Term:
    """
    _Term_

    Base Class for Terms in the predicate expression,
    contains the symbol that is used to id the term
    """
    def __init__(self, symbol):
        self.symbol = symbol


class Operand(Term):
    """
    _Operand_

    Base class for an Operand Term
    """
    def __init__(self, symbol):
        Term.__init__(self, symbol)
    

class FunctionTerm(Operand):
    """
    _FunctionTerm_

    Operand Term for containing a function in the predicate,
    this function needs to be mapped to a real predicate
    evaluation function that operates on a node and
    return some value
    """
    def __init__(self, symbol):
        Operand.__init__(self, symbol)
        self.args = []
        self._FunctionRef = None
        
    def __str__(self):
        """return a string representation of this Term"""
        result =  "FunctionTerm:%s(" % self.symbol
        for item in self.args:
            result += "%s," % item
        result += ")"
        return result

    def loadFunction(self):
        """
        _loadFunction_

        Load the function matching the symbol, raising an
        exception if it is not found
        """
        self._FunctionRef = PredicateFunctions.getPredicateFunction(
            self.symbol
            )
        if self._FunctionRef == None:
            msg = "Unknown Function: %s\n" % self.symbol
            msg += "Unable to find a function named %s\n" % self.symbol
            raise IMProvException(msg, ClassInstance = self,
                                  MissingFunction = self.symbol)
        return
        
    def __call__(self, node):
        """
        _operator()_

        Define operation of a FunctionTerm on a node,
        this looks the appropriate function from the
        Predicate Function Implementation table by matching the
        function name to the appropriate function, so that it can be evaluated
        on a node.
        """
        try:
            return self._FunctionRef(node, *self.args)
        except StandardError, ex:
            msg = "Error evaluating function: %s\n" % self.symbol
            msg += "With Arguments: %s\n" % ( self.args, ) 
            msg += str(ex)
            raise IMProvException(msg, ClassInstance = self,
                                  PredicateFunction = self.symbol)

    def appendArgument(self, newArg):
        """
        _appendArgument_

        add a new argument for the function, remove leading and trailing
        quotes from strings
        """
        if type(newArg) == type("string"):
            if newArg.startswith("\""):
                newArg = newArg[1:]
            if newArg.endswith("\""):
                newArg = newArg[:-1]
        self.args.append(newArg)
        return
    
class ValueTerm(Operand):
    """
    _ValueTerm_

    Operand Term containing a value provided explicitly in the expression
    such as a value for comparison
    """
    def __init__(self, symbol):
        Operand.__init__(self, symbol)

        
    def __str__(self):
        """return a string repr of this ValueTerm"""
        return "ValueTerm:%s" % self.symbol

    def __call__(self, node):
        """
        _operator()_

        evaluate the value, returning it as the appropriate type.
        If it is an int or boolean, then convert it to the actual
        type else leave it as a string
        """
        if self.symbol == "\"True\"":
            return True
        if self.symbol == "\"False\"":
            return False
        try:
            value = int(self.symbol)
            return value
        except ValueError:
            return self.symbol.replace('\"','')
        

class Operator(Term):
    """
    _Operator_

    Operand Term containing an operator symbol that will operate on
    terms to its left and right
    """
    def __init__(self, symbol):
        Term.__init__(self, symbol)
        self._FunctionRef = None
        
    def __str__(self):
        """create a string repr of this object"""
        result = "Operator:%s" % self.symbol
        return result

    def loadOperator(self):
        """
        _loadOperator_

        Get the matching operator implementation for this operator
        raise an error if no match
        
        """
        self._FunctionRef = PredicateOperators.getPredicateOperator(
            self.symbol
            )
        if self._FunctionRef == None:
            msg = "Unknown Operator: %s\n" % self.symbol
            msg += "Unable to find implementation for operator: "
            msg += "%s\n" % self.symbol
            raise IMProvException(msg, ClassInstance = self,
                                  MissingOperator = self.symbol)
        return
    
class ExprNode:
    """
    _ExprNode_

    Binary Tree Node, that combines two terms and an
    operator:
              +
             / \
            1   2

    """
    def __init__(self):
        self.parent = None
        self.leftHandSide = None
        self.rightHandSide = None
        self.operator = None
        self.operatorFunction = None

    def closed(self):
        """
        _closed_

        Return the state of this node as open or closed
        based on wether all its values have been filled
        """
        return self.leftHandSide and self.rightHandSide
    
    def addValue(self, value):
        """
        _addValue_

        Add a value to this operator, if lhs not set, then
        set the lhs to be the value, else, set the rhs
        """
        if self.leftHandSide == None:
            self.leftHandSide = value
            return
        elif self.rightHandSide == None:
            self.rightHandSide = value
            return

    def __call__(self, node):
        """
        _operator()_

        Evaluate this expression on the node provided,
        if the operator is set, then evaluate the operator,
        else, evaluate the left hand side value itself
        """
        if self.operator != None:
            try:
                return self.operatorFunction(
                    self.leftHandSide(node),
                    self.rightHandSide(node))
            except StandardError, ex:
                msg = "Error evaluating operator %s:\n" % self.operator.symbol
                msg += str(ex)
                raise IMProvException(msg, ClassInstance = self)
            
        return self.leftHandSide(node)
        
    def __str__(self):
        """create a string repr of this object"""
        result = "ExprNode:<%s, %s, %s>" % (self.leftHandSide, self.operator,
                                            self.rightHandSide)
        return result

    
class GlyphParser:
    """
    _GlyphParser_

    Event driven parser that stores an event for each glyph type

    """
    _MatchFunc = re.compile("^[a-zA-Z0-9_]+$")
    _MatchValue = re.compile("^\"[a-zA-Z0-9_]+\"$")
    _MatchOperator = re.compile("(^==$)|(^!=$)|(^>=$)|(^<=$)|(^\&\&$)|(^\|\|$)")
    
    def __init__(self):
        pass
    
        
    def glyphType(self, glyph):
        """
        Evaluate the type of glyph and return a 
        type value for the glyph that matches the types
        stored in this object
        """
        if glyph == "[":
            return "OpenPredicate"
        if glyph == "]":
            return "ClosePredicate"
        if glyph == "(":
            return "OpenParen"
        if glyph == ")":
            return "CloseParen"
        if self._MatchOperator.match(glyph):
            return "Operator"
        if self._MatchValue.match(glyph):
            return "Value"
        if self._MatchFunc.match(glyph):
            return "Function"
        msg = "Badly formed token: %s" % glyph
        raise IMProvException(msg, ClassInstance = self,
                              Token = glyph) 

    def __call__(self, glyphs):
        """
        _operator()_

        process a list of glyphs and convert them into
        and Expression tree
        
        """
        self.validateGlyphs(glyphs)
        newGlyphs = self.reduceFunctions(glyphs)
        newGlyphs = self.reduceValues(newGlyphs)
        newGlyphs = self.reduceOperators(newGlyphs)
        return self.createTree(newGlyphs)

    def validateGlyphs(self, glyphs):
        """
        _validateGlyphs_

        Basic validation of the predicate expressions

        """
        exprStr = ""
        parenStack = []
        for glyph in glyphs:
            exprStr += " %s " % glyph
            try:
                gType = self.glyphType(glyph)
            except IMProvException, ex:
                exprStr += "<<< Error"
                ex.addInfo(ErrorLocation = exprStr)
                raise ex
            if gType == "OpenParen":
                parenStack.append("(")
            if gType == "CloseParen":
                parenStack.pop()
        if parenStack != []:
            msg = "Mismatched Parentheses in Predicate expression:\n"
            msg += exprStr
            raise IMProvException(msg, ClassInstance = self,
                                  Expression = exprStr)
        return
        
    def reduceFunctions(self, glyphs):
        """
        _reduceFunctions_

        reduce all function ( *args ) combinations to FunctionTerm
        Objects
        
        """
        result = []
        currentFunc = None
        for item in glyphs:
            gType = self.glyphType(item)
            if gType == "Function":
                currentFunc = FunctionTerm(item)
                continue
            elif (gType == "OpenParen") and (currentFunc != None):
                #print "open Parentheses"
                continue
            elif (gType == "CloseParen") and (currentFunc != None):
                #print "close Parentheses"
                result.append(currentFunc)
                currentFunc.loadFunction()
                currentFunc = None
                continue
            elif (gType == "Value") and (currentFunc != None):
                #print "Value: ", item
                currentFunc.appendArgument(item) 
                continue
            else:
                result.append(item)
            
        return result
    
    def reduceValues(self, glyphs):
        """
        _reduceValues_

        Reduce all value glyphs to ValueTerm objects
        """
        result = []
        for item in glyphs:
            if type(item) != type("string"):
                result.append(item)
                continue
            gType = self.glyphType(item)
            if gType == "Value":
                result.append(ValueTerm(item))
            else:
                result.append(item)
        return result

    def reduceOperators(self, glyphs):
        """
        _reduceOperators_

        Swap out operators for Operator Term objects
        
        """
        result = []
        for item in glyphs:
            if type(item) != type("string"):
                result.append(item)
                continue
            gType = self.glyphType(item)
            if gType == "Operator":
                newOp = Operator(item)
                newOp.loadOperator()
                result.append(newOp)
                
            else:
                result.append(item)
        return result  



    def createTree(self, glyphs):
        """
        _createTree_

        Build a binary tree of ExprNode objects representing the
        operator/operand structure of the tree
        """
        result = ExprNode()
        current = result
        for glyph in glyphs:
            if glyph == "(":
                newNode = ExprNode()
                if not current.closed():
                    current.addValue(newNode)
                    newNode.parent = current
                    current = newNode
                    continue
                else:
                    current.parent = newNode
                    newNode.addValue(current)
                    current = newNode
                    continue
            elif glyph == ")":
                current = current.parent
                continue
            elif isinstance(glyph, Operand):
                current.addValue(glyph)
                continue
            elif isinstance(glyph, Operator):
                current.operator = glyph
                current.operatorFunction = glyph._FunctionRef
                continue
        while current.parent != None:
            current = current.parent
        return current



  

def parsePredicate(predicateExpr):
    """
    _parsePredicate_

    Parse, convert predicate expression string into a
    predicate object tree
    """
    exprParser = ExpressionParser(predicateExpr)
    try:
        glyphs = exprParser.parse()
    except StandardError, ex:
        msg = "Error parsing predicate expression:\n"
        msg += str(predicateExpr)
        msg += "Error: %s" % str(ex)
        raise IMProvException(msg, ClassInstance = exprParser,
                              Expression = predicateExpr)
    
    glyphParser = GlyphParser()
    try:
        return glyphParser(exprParser.glyphs)
    except IMProvException, ex:
        ex.addInfo(Expression = predicateExpr)
        raise ex
    except StandardError, ex:
        msg = "Error parsing predicate expression:\n"
        msg += str(predicateExpr)
        msg += "Error: %s" % str(ex)
        raise IMProvException(msg, ClassInstance = glyphParser,
                              Expression = predicateExpr)
    



            

