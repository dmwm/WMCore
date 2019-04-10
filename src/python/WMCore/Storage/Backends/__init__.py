#!/usr/bin/env python
"""
_StageOut.Impl_

Implementations of the StageOutImpl for specific protocols

"""
from __future__ import absolute_import


__all__ = []

#  //
# // Each plugin should contain the Registration call at module level
#//  and be imported here, so that all the StageOut plugins can be
#  //imported automatically
# //
#//

from WMCore.Storage.Backends import CPImpl
from WMCore.Storage.Backends import FNALImpl
from WMCore.Storage.Backends import SRMV2Impl
from WMCore.Storage.Backends import LCGImpl
from WMCore.Storage.Backends import XRDCPImpl
from WMCore.Storage.Backends import VandyImpl
from WMCore.Storage.Backends import GFAL2Impl
from WMCore.Storage.Backends import AWSS3Impl
from WMCore.Storage.Backends import UnittestImpl
from WMCore.Storage.Backends import TestFallbackToOldBackend
