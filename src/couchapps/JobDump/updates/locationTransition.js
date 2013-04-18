/**
 * Implement an update document to update a transition's location
 * @author dballest
 */
function (doc, req) {
    if (!doc) {
        return [doc, 'FAIL']
    };

    var newLocation = req.query['location'];

    var maxKey = 0;
    for (key in doc.states) {
        if (maxKey < parseInt(key)) {
            maxKey = parseInt(key);
        }
    }

    doc.states[(maxKey) + ""]['location'] = newLocation
    return [doc, 'OK'];
}