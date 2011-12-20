function(doc) {
  emit([doc.Campaign, doc.RequestName],
        {'_id': doc['_id'], 'requestor' : doc['Requestor'], 'inputDataset' : doc['InputDataset'],
         'requestname' : doc['RequestName'], 'inputDBS' : doc['DbsUrl'],
         'requestorDN' : doc['RequestorDN']
        }
      );
}