function(head, req) {
  var row;
  start({
    "headers": {
      "Content-Type": "text/json"
     }
  });
  while(row = getRow()) {
    send(row.value);
  }  
}

