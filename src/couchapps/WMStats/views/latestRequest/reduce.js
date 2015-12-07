function(keys, values, rereduce) {
    var latestRequest = values[0];
    for (var i = 1; i < values.length; i++) {
        if (latestRequest.timestamp < values[i].timestamp) {latestRequest = values[i]};
    }
    return latestRequest;
}