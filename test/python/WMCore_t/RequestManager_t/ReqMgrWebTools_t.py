from WMCore.HTTPFrontEnd.RequestManager.ReqMgrWebTools import parseRunList, parseBlockList, removePasswordFromUrl

l0 = ''
l1 = ' [1,  2,  3 ] '
l2 = '1,  2, 3   '
assert(parseRunList(l0) == [])
assert(parseRunList(l1) == [1,2,3])
assert(parseRunList(l2) == [1,2,3])

l3 = '  ["Barack", "  Sarah  ",George]'
assert(parseBlockList(l3) == ['Barack', 'Sarah', 'George'])

url = 'http://sarah:maverick@whitehouse.gov:1600/birthcertificates/trig'
cleanedUrl = removePasswordFromUrl(url)
assert(cleanedUrl == 'http://whitehouse.gov:1600/birthcertificates/trig')

