/**
 * Given a request name obtain the Resubmission requests where request is the input
 * collection.
 * @author Diego Ballesteros
 */
function(doc){
    if(doc["OriginalRequestName"]){
        emit(doc["OriginalRequestName"], null);
    }
}