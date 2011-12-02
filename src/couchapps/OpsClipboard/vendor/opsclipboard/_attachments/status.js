// status.js
// 
// Definition of requests possible statuses in the OpsClipboard application.
// (status, state used interchangeably)
//
// Basically each state has a list of other states it can go into.
//
// ReadyToRelease and ReadyToReject mean that the request is done in
// the OpsClipboard so there are no further states.
//
// Naming convention: ReqMgr states lower-case-dashed
//                   OpsClipboard states: CamelCase

// nomenclature of variables is a bit clumsy, but follows RequestStatus.py
// which defines ReqMgr request status names and transitions.
var requestStatus = 
{    
    // keys of this map are the valid states
    // values are list of allowed states that the state can move into
    statusList: 
    {
        // initial state
        "NewlyHeld" : ["Prestaging", "TapeFamilies", "ReadyToRelease", "ReadyToReject"],
        // intermediate states
        "Prestaging" : ["ReadyToRelease", "ReadyToReject"],
        "TapeFamilies" : ["TapeFamiliesDone", "ReadyToRelease", "ReadyToReject"],
        "TapeFamiliesDone" : ["Prestaging", "ReadyToRelease", "ReadyToReject"],
        "WaitingForTransfers" : [ "Prestaging", "ReadyToRelease", "ReadyToReject"],
        
        // final states, either good to go, or can't go & needs to be removed:
        // any requests in the release view means that they are done in the
        // OpsClipboard and need to be advanced to the next state in the ReqMgr
        "ReadyToRelease" : [],
        // any requests in the "ReadyToReject" state mean that they are
        // done in the OpsClipboard and need to be aborted in the ReqMgr
        "ReadyToReject"  : [],
    },
}