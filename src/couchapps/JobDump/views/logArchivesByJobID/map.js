function(doc) {
  if (doc['type'] == 'fwjr') {
    for (var stepName in doc['fwjr'].steps) {
      if (stepName != 'logArch1') {
        continue;
      }

      var step = doc['fwjr']['steps'][stepName];      
      for (var stepAttribute in step) {
        if (stepAttribute != 'output') {
          continue;
        }

        for (var outputModule in step[stepAttribute]) {
          if (outputModule != 'logArchive') {
            continue;
          }

          if (step[stepAttribute][outputModule].length > 0 &&
              'location' in step['output']['logArchive'][0]) {
            emit([doc['jobid'], doc['retrycount']],
                {'lfn': step["output"]["logArchive"][0]["lfn"],
                 'retrycount': doc['retrycount'],
                 'location': step["output"]["logArchive"][0]["location"]});
          }
        }
      }
    }
  }
}
