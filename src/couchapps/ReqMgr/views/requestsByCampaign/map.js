function(user_request) {
  if (user_request.Requestor && user_request.Campaign) {
    emit([user_request.Campaign],
        {'_id': user_request['_id'], 'RequestName' : user_request['RequestName'], 'OriginalRequestName' : user_request['OriginalRequestName'], 'Submission' : user_request['Submission']}
        );
  }
}
