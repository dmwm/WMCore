function(user_request) {
  if (user_request.RequestDate && user_request.Requestor) {
    emit([user_request.Requestor, user_request.RequestDate],
        {'_id': user_request['_id'], 'RequestName' : user_request['RequestName'], 'OriginalRequestName' : user_request['OriginalRequestName'], 'Campaign': user_request['Campaign']}
        );
  }
}
