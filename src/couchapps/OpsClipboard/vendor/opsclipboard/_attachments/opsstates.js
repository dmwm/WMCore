//
//  States definition for ops clipboard
//
//
var opsstates = {
    
    //
    // keys of this map are the valid states
    // values are list of allowed states that the state can move into
    states : {
        // initial state
        "NewlyHeld" : ["Prestaging", "TapeFamilies", "ReadyToRelease", "ReadyToReject"],
        // final states, either good to go, or cant go & needs to be removed
        "ReadyToRelease" : [],
        "ReadyToReject"  : [],
        // intermediate states
        "Prestaging" : ["ReadyToRelease", "ReadyToReject"],
        "TapeFamilies" : ["TapeFamiliesDone", "ReadyToRelease", "ReadyToReject"],
        "TapeFamiliesDone" : ["Prestaging", "ReadyToRelease", "ReadyToReject"],
        "WaitingForTransfers" : [ "Prestaging", "ReadyToRelease", "ReadyToReject"],
    },
    
    
    
}