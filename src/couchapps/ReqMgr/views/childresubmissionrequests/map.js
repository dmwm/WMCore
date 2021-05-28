/**
 * Given a request name obtain the Resubmission requests where request is the input
 * collection.
 * @author Diego Ballesteros
 */
function(doc){
    if(doc.InitialTaskPath){
        emit(doc.InitialTaskPath.split("/")[1], doc.RequestStatus);
    }
}
