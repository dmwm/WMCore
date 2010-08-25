#!/usr/bin/env python
"""
_StageOut.Impl_

Implementations of the StageOutImpl for specific protocols

"""


__all__ = []

#  //
# // Each plugin should contain the Registration call at module level
#//  and be imported here, so that all the StageOut plugins can be
#  //imported automatically
# //
#//

import WMCore.Storage.Plugins.StageOutUnitTest
import WMCore.Storage.Plugins.CPImpl
import WMCore.Storage.Plugins.LCGImpl
import WMCore.Storage.Plugins.DCCPFNALImpl


