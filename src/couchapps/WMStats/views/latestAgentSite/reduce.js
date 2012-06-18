function(keys, values, rereduce) {
    var latestTime = values[0];
    for (var i = 1; i < values.length; i++) {
        if (latestTime < values[i]) {latestTime = values[i]};
    }
    return lastestTime;
}