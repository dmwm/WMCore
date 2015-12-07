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

from . import SRMImpl
from . import DCCPFNALImpl
from . import FNALImpl
from . import DCCPGenericImpl
from . import RFCPImpl
from . import RFCP1Impl
from . import RFCP2Impl
from . import RFCPCERNImpl
from . import RFCPRALImpl
from . import PYDCCPImpl
from . import SRMV2Impl
from . import XRDCPImpl
from . import CPImpl
from . import LCGImpl
from . import HadoopImpl
from . import UnittestImpl
from . import VandyImpl
from . import TestFallbackToOldBackend
from . import GFAL2Impl
