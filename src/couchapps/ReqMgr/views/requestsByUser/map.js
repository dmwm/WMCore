function(doc) {
  emit([doc.Requestor, doc.PublishDataName, doc.InputDataset, doc.ProcessingVersion],
        {'_id': doc['_id'], 'requestor' : doc['Requestor'], 'version' : doc['ProcessingVersion']}
      );
}