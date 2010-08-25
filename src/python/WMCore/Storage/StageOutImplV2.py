#!/usr/bin/env python
"""
_StageOutImplV2_

Interface for Stage Out Plugins. All stage out implementations should
inherit this object and implement the methods accordingly

"""


class StageOutImplV2:
    """
    _StageOutImplV2_

    """

    def __init__(self):
        pass
    
    def doTransfer(self, lfn, pfn, stageOut, seName, command, options, protocol  ):
        """
            performs a transfer. stageOut tells you which way to go. returns the new pfn or
            raises on failure. StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        raise NotImplementedError
    
    def doDelete(self, lfn, pfn, seName, command, options, protocol  ):
        """
            deletes a file, raises on error
            StageOutError (and inherited exceptions) are for expected errors
            such as temporary connection failures. Anything else will be handled as an unexpected
            error and skip retrying with this plugin
        """
        raise NotImplementedError
