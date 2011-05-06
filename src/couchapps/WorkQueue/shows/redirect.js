function(doc, req) {
  // redirect from _rewrite to index.html - only call this from _rewrite
  // See http://wiki.apache.org/couchdb/Throw%20a%20404%20or%20a%20redirect
  if (req.headers['Cms-Request-Uri']) {
	  // we are behind a cms frontend which is proxying requests
	  var http = req.headers.Https === 'on' ? 'https://' : 'http://';
	  var uri = req.headers['Cms-Request-Uri'];
	  if (uri.length > 0) {
	      uri = uri[uri.length - 1] === '/' ? uri : uri + '/';
	  }
	  var location = http + req.headers['X-Forwarded-Host'] + uri + 'index.html';
  } else {
	  // assemble uri from path - assume http
	  req.path.pop(); req.path.pop(); // need to remove '_show/redirect' from uri
	  var location = 'http://' + req.headers.Host + '/' + req.path.join('/') + '/_rewrite/index.html';
  }

  var redirect = {code : 301,
		          headers : {"Location" : location
		                    }
                 };
  return redirect;
}