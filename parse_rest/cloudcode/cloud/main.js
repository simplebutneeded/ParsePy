
// Use Parse.Cloud.define to define as many cloud functions as you want.
// For example:
Parse.Cloud.define("hello", function(request, response) {
  response.success("Hello world!");
});


Parse.Cloud.define("averageStars", function(request, response) {
  var query = new Parse.Query("Review");
  query.equalTo("movie", request.params.movie);
  query.find({
    success: function(results) {
      var sum = 0;
      for (var i = 0; i < results.length; ++i) {
        sum += results[i].get("stars");
      }
      response.success(sum / results.length);
    },
    error: function() {
      response.error("movie lookup failed");
    }
  });
});

Parse.Cloud.define("sessionForUser", function(request, response) {
    Parse.Cloud.useMasterKey();
    
    var query = new Parse.Query(Parse.User);
    query.get(request.params.userId).then(
      function(object) {
        response.success({"session":object.getSessionToken()})
      },
      function(error) {
        response.error({"detail":"User not found:"+error.message})
      }
    );
});

