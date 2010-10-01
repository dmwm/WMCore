from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import parseRunList, parseBlockList

l0 = ''
l1 = ' [1,  2,  3 ] '
l2 = '1,  2, 3   '
assert(parseRunList(l0) == [])
assert(parseRunList(l1) == [1,2,3])
assert(parseRunList(l2) == [1,2,3])

l3 = '  ["Barack", "  Sarah  ",George]'
assert(parseBlockList(l3) == ['Barack', 'Sarah', 'George'])

