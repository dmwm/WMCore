function(doc) {
    if ((doc.type == "jobsummary") && (doc.state == "success")) {
        for (outfile in doc.output) {
            emit([doc.workflow, doc.output[outfile]['type'], doc.timestamp], {'jobid': doc._id,
                                                                              'checksums': doc.output[outfile]['checksums'],
                                                                              'size': doc.output[outfile]['size'],
                                                                              'location': doc.output[outfile]['location'],
                                                                              'lfn': doc.output[outfile]['lfn']} );
        }
    }
}
