# needs 1 argument - ReqMgr URL:
#	http://localhost:8246 when running on localhost
#	https://localhost:2000 when running against ReqMgr on VM via ssh tunnel

if [ "x$1" == "x" ] ; then
	echo "Missing ReqMgr URL (e.g. http://localhost:8246)"
	exit 1
fi

REQMGR_URL=$1
py test/data/ReqMgr/reqmgr2.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/ReReco.json --json '{"createRequest": {"InputDataset": "/BTag/Run2011B-v1/RAW"}}' --all_tests && \
py test/data/ReqMgr/reqmgr2.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChain.json --all_tests && \
py test/data/ReqMgr/reqmgr2.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainMultiDQM.json --all_tests && \
py test/data/ReqMgr/reqmgr2.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainMultiTask.json --all_tests && \
py test/data/ReqMgr/reqmgr2.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainPileupScratch.json --all_tests && \
py test/data/ReqMgr/reqmgr2.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainProdTTbar.json --all_tests && \
py test/data/ReqMgr/reqmgr2.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainRunMET2012A.json --all_tests
