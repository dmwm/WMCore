from WMCore.RequestManager.Clipboard.Insert import getRequestsInState

if __name__ == '__main__':
    reqmgr = "http://vocms144.cern.ch:8687"
    reqs = getRequestsInState(reqmgr, u'running')
    inject(os.environ['COUCHURL'], "opsclip", *reqs)

