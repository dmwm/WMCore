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

import SRMImpl
import DCCPFNALImpl
import DCCPGenericImpl
import RFCPImpl
import RFCP1Impl
import RFCP2Impl
import RFCPCERNImpl
import RFCPRALImpl
import PYDCCPImpl
import SRMV2Impl
import XRDCPImpl
import CPImpl
import LCGImpl
import HadoopImpl
import UnittestImpl

