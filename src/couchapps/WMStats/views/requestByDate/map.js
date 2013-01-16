
function(doc) {
  if (doc.request_date && (doc.request_date.length > 0)){
      var dateArray = doc.request_date;
      // get year, month, day, min
      emit([dateArray[0], dateArray[1], dateArray[2], dateArray[3]], null) ;
  } 
}
