#!/usr/bin/env python
"""
WorkQueue End Policy

"""
__revision__ = "$Id: __init__.py,v 1.1 2009/12/02 13:52:45 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMFactory import WMFactory
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

endFac = WMFactory(__name__, __name__)

def endPolicy(elements, args):
    """Load an end policy"""

    for ele in elements:
        # if still working return simple aggregate state
        if not ele.inEndState():
            return endFac.loadObject('SingleShot',
                             {'SuccessThreshold' : 1.0},
                             storeInCache = False)(*elements)

    # all elements finished processing load policy and apply
    spec = WMWorkloadHelper()
    spec.load(elements[0]['WMSpecUrl'])
    name = spec.data.policies.end.policyName
    args = args.get(name, {})
    args.update(spec.data.policies.end.dictionary_())
    return endFac.loadObject(name,
                             args,
                             storeInCache = False)(*elements)

__all__ = [endPolicy]
