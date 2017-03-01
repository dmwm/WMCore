import time
import random

def generate_reqmgr_requests(workflowList):
    """
    generate the request with following structure
      {
       "_id": "cmsdataops_sryu_test4_120111_114950_128",
       "_rev": "6-02b17b4eabcf333e7499c0fa0ae5055b",
       "inputdataset": "/Photon/Run2011A-v1/RAW",
       "group": "cmsdataops",
       "request_date": [2012, 1, 11, 17, 49, 50],
       "campaign": "SryuTest2",
       "workflow": "cmsdataops_sryu_test4_120111_114950_128",
       "priority": "1",
       "requestor": "cmsdataops",
       "request_type": "ReReco",
       "type": "reqmgr_request",
       "request_status": [
           {
               "status": "new",
               "update_time": 1326304190
           },
           {
               "status": "assignment-approved",
               "update_time": 1326304216
           },
           {
               "status": "assigned",
               "update_time": 1326304227
           }
       ],
       "site_white_list": [
           "T1_DE_KIT"
       ],
       "teams": [
           "cmsdataops"
       ]
    }
    """
    docs = []
    for workflow in workflowList:
        doc = {"_id": workflow,
               "inputdataset": "/Fake/input/data",
               "group": "cmsdataops",
               "request_date": [2012, 9, 12, 0, 0, 0],
               "campaign": "FakeCampaign",
               "workflow": workflow,
               "priority": "1",
               "requestor": "cmsdataops",
               "request_type": "Fake",
               "type": "reqmgr_request",
               "request_status": [
                                  {"status": "assignment-approved", "update_time": 10000000}
                                 ],
                "site_white_list": ["Fake"],
                "teams": ["cmsdataops"]
                }
        docs.append(doc)
    return docs

if __name__ == "__main__":
    import json
    reqmgr_requests = generate_reqmgr_requests(["etorassa_EXO-Summer12_DR53X-00003_T1_FR_CCIN2P3_MSS_batch22_v1__120817_181454_6181"])

    docList = []
    docList.extend(reqmgr_requests)
    docs = {"docs": docList};

    json.dump(docs, open("sample_docs.json", "w+"))
    #use this
    #curl -d @sample_docs.json -X POST -H "Content-Type:application/json" $DB_URL/_bulk_docs
