#!/usr/bin/env python
"""
WorkQueue Policy

https://twiki.cern.ch/twiki/bin/view/CMS/WMCoreWorkQueuePolicy
"""


#
#from WMCore.WMFactory import WMFactory
#from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
#
#startFac = WMFactory("WorkQueueStartPolicy", __name__ + '.Start')
#endFac = WMFactory("WorkQueueEndPolicy", __name__ + '.End')
#startMap = {}
#
##TODO: Argument handling need to give params from workqueue as well as from spec
#
#def setStartPolicy(policyDirective, policyName, policyArgs):
#    """Set the Start policy for a queue"""
#    global startMap
#    startMap[policyDirective] = (policyName, policyArgs)
#
#def startPolicy(name, startMap):
#    """Load a start policy"""
#    # Do we have a defined policy for this policy - if not load null policy
#    if not startMap.has_key(name):
#        name = 'passthrough'
#    policyName, args = startMap[name]
#    #policyName, args = startMap.get(name, ('passthrough', {}))
#    return startFac.loadObject(policyName,
#                               args,
#                               storeInCache = False,
#                               )
##    return startFac.loadObject(startMap.get(name, name),
##                               args,
##                               storeInCache = False)
#
#def endPolicy(elements, args):
#    """Load an end policy"""
#
#    for ele in elements:
#        # if still working return simple aggregate state
#        if not ele.inEndState():
#            return endFac.loadObject('SingleShot',
#                             {'SuccessThreshold' : 1.0},
#                             storeInCache = False)(*elements)
#
#    # all elements finished processing load policy and apply
#    spec = WMWorkloadHelper()
#    spec.load(elements[0]['WMSpecUrl'])
#    args.update(spec.data.policies.end.dictionary_())
#    return endFac.loadObject(spec.data.policies.end.policyName,
#                             args,
#                             storeInCache = False)(*elements)
#
#__all__ = [setStartPolicy, startPolicy, endPolicy]
