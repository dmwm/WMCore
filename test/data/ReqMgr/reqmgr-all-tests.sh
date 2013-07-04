# needs 1 argument - ReqMgr URL:
#	http://localhost:8687 when running on localhost
#	https://localhost:2000 when running against ReqMgr on VM via ssh tunnel
if [ "x$1" == "x" ] ; then
	echo "Missing ReqMgr URL (e.g. http://localhost:8687)"
	exit 1
fi

REQMGR_URL=$1
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/MonteCarlo.json  --json '{"createRequest": {"Requestor": "maxa"}, "assignRequest": {"Team": "dmwm", "AcquisitionEra": "AcquisitionEraTest"}}' --allTests && \
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/MonteCarloFromGEN.json  --json '{"createRequest": {"Requestor": "maxa", "InputDataset": "/QCD_HT-1000ToInf_TuneZ2star_8TeV-madgraph-pythia6/Summer12-START50_V13-v1/GEN"}, "assignRequest": {"Team": "dmwm", "AcquisitionEra": "AcquisitionEraTest"}}' --allTests && \
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/ReDigi.json  --json '{"createRequest": {"Requestor": "maxa", "InputDataset": "/WprimeToENu_M-3000_TuneZ2star_8TeV-pythia6/Summer12-START50_V13-v1/GEN-SIM"}, "assignRequest": {"Team": "dmwm", "AcquisitionEra": "AcquisitionEraTest"}}' --allTests && \
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/ReReco.json  --json '{"createRequest": {"Requestor": "maxa", "InputDataset": "/BTag/Run2011B-v1/RAW"}, "assignRequest": {"Team": "dmwm", "AcquisitionEra": "AcquisitionEraTest"}}' --allTests && \
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChain.json --json '{"createRequest": {"Requestor": "maxa"}, "assignRequest": {"Team": "dmwm", "AcquisitionEra": "AcquisitionEraTest"}}' --allTests && \
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainMultiDQM.json --json '{"createRequest": {"Requestor": "maxa"}, "assignRequest": {"Team": "dmwm"}}' --allTests && \
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainMultiTask.json --json '{"createRequest": {"Requestor": "maxa"}, "assignRequest": {"Team": "dmwm"}}' --allTests && \
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainPileupScratch.json --json '{"createRequest": {"Requestor": "maxa"}, "assignRequest": {"Team": "dmwm"}}' --allTests && \
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainProdTTbar.json --json '{"createRequest": {"Requestor": "maxa"}, "assignRequest": {"Team": "dmwm"}}' --allTests && \
py test/data/ReqMgr/reqmgr.py -u $REQMGR_URL -f ./test/data/ReqMgr/requests/TaskChainRunMET2012A.json --json '{"createRequest": {"Requestor": "maxa"}, "assignRequest": {"Team": "dmwm"}}' --allTests