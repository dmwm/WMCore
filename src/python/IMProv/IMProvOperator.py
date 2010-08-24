#!/usr/bin/env python
"""
_IMProvOperator_

Base class for implementing IMProvNode tree traversal in a top down
recursive descent manner, similar to a SAX parser.

"""


class IMProvOperator:
    """
    _IMProvOperator_

    Event driven traversal of an IMProvNode tree similar to the SAX parser
    model.

    """
    def __init__(self):
        pass



    def startNode(self, nodename, nodeRef):
        """
        _startNode_

        Start of an IMProvNode. Override to act on the start of a node

        """
        pass

    def endNode(self, nodename, nodeRef):
        """
        _endNode_

        End of a node. Override to act at the end of a node

        """
        pass


    def __call__(self, improvNode):
        """
        _operator()_

        Act on the node provided and traverse it triggering the appropriate
        callbacks at the start and end of each node

        Do not override
        """
        improvNode.improvOperator(self)
        return
        

    
    
