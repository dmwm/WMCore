function(doc) {
    if (doc.Step1 && doc.Step1.IncludeParents === true){
        emit(doc.RequestStatus, doc.Step1.InputDataset);
    }
    if (doc.Task1 && doc.Task1.IncludeParents === true){
        emit(doc.RequestStatus, doc.Task1.InputDataset);
    }
    if (doc.IncludeParents === true){
        emit(doc.RequestStatus, doc.InputDataset);
    }
}