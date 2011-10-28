function(doc) {
  if (doc['type'] == 'fwjr') {
    for (var stepName in doc['fwjr'].steps) {
      if (stepName != 'logArch1') {
        continue;
      }
      var request = doc['fwjr'].task.split('/')[1]

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
              'pfn' in step['output']['logArchive'][0]) {
            emit([request], {"jobid" : doc['jobid'],
                               "lfn" : step["output"]["logArchive"][0]["lfn"],
                               "location" : step["output"]["logArchive"][0]["location"],
                         "checksums" : step["output"]["logArchive"][0]["checksums"]});
          }
        }
      }
    }
  }
}
