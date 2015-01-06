function cleanConfirmation () {
  var doc = document.getElementById('confirmation');
  doc.innerHTML='';
  doc.className='';
}
function errorMessage(err) {
  var doc = document.getElementById('confirmation');
  var html = '<div><button class="btn btn-small btn-blue right" onclick="javascript:cleanConfirmation()">Close</button>';
  html += err.replace(/\n/g, '<br/>') ;
  html += '</div>'
  doc.innerHTML=html;
  doc.className='tools-alert tools-alert-red confirmation shadow';
}
function ajaxRequest(path, parameters) {
    // path is an URI binded to certain server method
    // parameters is dict of parameters passed to the server function
    new Ajax.Updater('response', path,
    { method: 'post' ,
      parameters : parameters,
      onCreate: function() {
          var doc = document.getElementById('confirmation');
          doc.innerHTML='Your request has been submitted';
          doc.className='tools-alert tools-alert-blue confirmation fadeout shadow';
      },
      onException: function() {
          var doc = document.getElementById('confirmation');
          doc.innerHTML='ERROR! Your request has been failed';
          doc.className='tools-alert tools-alert-red confirmation fadeout shadow';
          setTimeout(cleanConfirmation, 5000);
      },
      onComplete : function(response) {
          var doc = document.getElementById('confirmation');
          if  (response.status==200 || response.status==201) {
              doc.innerHTML='SUCCESS! Your request has been processed with status '+response.status;
              doc.className='tools-alert tools-alert-green confirmation fadeout shadow';
              setTimeout(cleanConfirmation, 5000);
          } else {
              doc.innerHTML='WARNING! Your request has been processed with status '+response.status;
              doc.className='tools-alert tools-alert-yellow confirmation fadeout shadow';
              var headers = response.getAllResponseHeaders();
              errorMessage(headers);
          }
      }
    });
}
