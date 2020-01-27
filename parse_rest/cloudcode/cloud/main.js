Parse.Cloud.define("sessionForUser", function(request, response) {

    if (!request.master) {

        response.error({
            "detail": "must be called with master key"
        });

    }

    var query = new Parse.Query(Parse.User);

    query.get(request.params.userId, {
        useMasterKey: true
    }).then(

        function(object) {

            //console.log(object);

            var token = object.getSessionToken({
                useMasterKey: true
            });

            if (token) {

                response.success({
                    "session": object.getSessionToken({
                        useMasterKey: true
                    })
                });

            } else {

                var _ = require('underscore');
                //var Buffer = require('buffer').Buffer;
                password = new Buffer(24);

                _.times(24, function(i) {
                    password.set(i, _.random(0, 255));
                });

                password = password.toString("base64");

                //console.log(password);

                var res = object.setPassword(password, {
        useMasterKey: true
    });
                //var resAsString = res.toString;
                console.log("BEFORE");
                console.log(res);
                console.log("AFTER");
                //console.log(resAsString);

                /*if (!res) {
                    response.error({
                        "detail": "unable to set password"
                    });
                    return;
                }*/

                object.save(null, {
                    useMasterKey: true
                }).then(function(object) {

                    // Call Parse Login function with those variables
                    return Parse.User.logIn(object.get("username"), password); // {

                }).then(function(object) {

                    // Return the user's revokable session.
                    response.success({
                        "session": object.getSessionToken({
                            useMasterKey: true
                        })
                    });

                }, function(error) {

                    response.error({
                        "detail": "User login failed:" + error.message
                    });

                });

            }

        },

        function(error) {

            response.error({
                "detail": "User not found:" + error.message
            });

        }

    );

});
Parse.Cloud.define("report_contents", function(request, response) {
  
  if (!request.user) {
    return response.error('not authorized');
  }

  var query = new Parse.Query('CustomReport');
  query.equalTo('objectId',request.params.reportId).find({useMasterKey: true,
    success: function(results) {
               if ( results.length == 0) {
                 response.error('Report not found');
                 return;
               }
               if (!results[0].get('file')) {
                 response.error('No file associated with report');
                 return;
               }
               URL = results[0].get('file').url();
               if(URL.substring(0, 27) === "http://files.parsetfss.com/"){
                    new_URL = "https://files.simplebutneeded.net/tfss/" + URL.substring(27);
                }
                else if(URL.substring(0, 24) === "http://files.parse.com/"){
                    new_URL = "https://files.simplebutneeded.net/files/" + URL.substring(24);
                } else {
                    new_URL = URL;
                }
               Parse.Cloud.httpRequest({ url: new_URL,
                                         success: function(file_response) {

                                                   response.success(file_response.text);
                                                  },
                                         error: function () {
                                                   response.error('Unable to retrieve file: '+url);
                                                  }
               }); // httpRequest
             }, // success
    error: function() {
               response.error('Unable to retrieve report');
           } // error
  }); // find
});
Parse.Cloud.beforeSave  ("CustomReport", function (request,response) {
    if (request.object.isNew()) {
        request.object.set('createdBy',request.user);
    } else {
        request.object.set('updatedBy',request.user);
    }
    response.success();
});
Parse.Cloud.define("sendFeedbackEmail", function(request, response) {

  if (!request.user) {
    return response.error('not authorized');
  }
    var user = request.user;
    var email = user.get("username");

    var fromString = "Web App Feedback Form <";
    fromString = fromString +email;
    fromString = fromString +">";
    //console.log(request.user);
    //console.log(email);
    //console.log(fromString);

    var mailgun = require('mailgun-js')({apiKey: "key-52g8ijiqs-543l26jwzqjektl4sh6892", domain:"simplebutneeded.net"});
    var data = {
                from:fromString,
                to:"Jon Noreika <jon@simplebutneeded.com>",
                subject:request.params.subject,
                text:request.params.body
               }
    mailgun.messages().send(data, function (err, body) {
                if (err) {
                    console.log("got an error in sendEmail: " + err);
                    response.error("Uh oh, something went wrong");
                } else {
                    console.log("email sent to " + toEmail + " " + new Date());
                    response.success("Email sent!");
                }
      });
    /*Mailgun.sendEmail({
        to: "Jon Noreika <jon@simplebutneeded.com>, Raj Maitra <raj.d.maitra@gmail.com>, Shourya Basu <shourya@simplebutneeded.com>, Ian Ranahan <ian@simplebutneeded.com>",
        from: fromString,
        subject: request.params.subject,
        text: request.params.body
    }, {
        success: function(httpResponse) {
            console.log(httpResponse);
            response.success("Email sent!");
        },
        error: function(httpResponse) {
            console.error(httpResponse);
            response.error("Uh oh, something went wrong");
        }
    });*/
});
Parse.Cloud.define("sendExceptionDetails", function(request, response) {
 
  if (!request.user) {
    return response.error('not authorized');
  }

  var Exception = Parse.Object.extend("Exception");
  var query = new Parse.Query(Exception);
      
  console.log("Here #1"); 
  query.equalTo("objectId", request.params.objectId);
  query.include("owner");
  query.include("location");
  query.include("creator");
  query.include("creator.userInfo");
  query.first( {useMasterKey: true,
      
    success : function(object) {
  
        if(object){
  
        // console.log("Exception already in parse");
        console.log(object);
        // console.log(object.id);
        // console.log("Creator Email");
        // console.log(object.get("creator").getEmail());
        // console.log("creator first and last");
        // console.log(object.get("creator").get("userInfo").attributes.firstName);
        // //console.log(object.get("creator"))


        console.log("SUCCESS: Exception found in parse!");
        //response.success("SUCCESS: Exception found in parse!");
         if (!request.user.getEmail()) {
            return response.error('sender email not available');
          } 
          if (!object.get("owner").getEmail()) {
            return response.error('exception owner email not available');
          } 
          if (!object.get("location")) {
            return response.error('e');
          } 


      var primary=object.get("location").attributes.primary;
      var secondary= object.get("location").attributes.secondary;
      var tertiary=object.get("location").attributes.tertiary;

      if(!tertiary) {
        if(!secondary){
          var loc= primary;
        }
        else{
           var loc= primary +" "+secondary;
        }
      }else{
        var loc= primary + " " + secondary + " " + tertiary;
      }

      var openDate=object.attributes.openDate;
      var dueDate= object.attributes.dueDate;

      var message="Unique ID: "+ object.attributes.uniqueID +"\n\n" + "Description: "+ object.attributes.description + "\n\n" + "Open Date: " + 
      openDate + "\n\n" +"Due Date: " + dueDate + "\n\n" +"Location: " + loc + "\n\n"+
      "Status: " + object.attributes.status + "\n\n" +
      "Priority: " + object.attributes.priority.toString() + "\n\n" +
      "Creator Name: " + object.get("creator").get("userInfo").attributes.firstName + " " + object.get("creator").get("userInfo").attributes.lastName +"\n\n" +
      "Creator Email: " + object.get("creator").getEmail()+"\n\n" + 
      "EXCEPTION HISTORY: \n\n";

      var history=object.attributes.history;
      for(i=0; i < history.length ; i++){
        var p = history[i].priority;
        if(!history[i].priority){
          var p=0;
        }
        message+= "Action: " + history[i].action + "\n\n" +
        "Action Date: " + history[i].actionDate + "\n\n" + 
        "Priority: " + p + "\n\n" + 
        "Status: " + history[i].status + "\n\n" + 
        "User: " + history[i].user.name + "\n\n"; 
      }

      var mailgun = require('mailgun-js')({apiKey: "key-52g8ijiqs-543l26jwzqjektl4sh6892", domain:"simplebutneeded.net"});
      var mail = {
                from:request.user.getEmail(),
                to:object.get("owner").getEmail(),
                subject:"Your Exception Reminders!",
                text:"We are sending this email to remind you of your upcoming exception:\n\n"+ message
                }

      mailgun.messages().send(mail, function (err, body) {
                if (err) {
                    console.log("got an error in sendEmail: " + err);
                    response.error("Uh oh, something went wrong");
                } else {
                    console.log("email sent to " + toEmail + " " + new Date());
                    response.success("Email sent!");
                }
      });
  
    } else {
  
        console.log("ERROR: Exception NOT found in parse!");
        response.error("ERROR: Exception NOT found in parse!");
  
  
      }
    },
    error : function(error) {
  
      // The user shouldn't see this error.
      console.error("There was a problem searching for the Exception.");
      // But let's make it informative anyways
      response.error("There was a problem searching for the Exception.");
  
    }
  });
  
});
Parse.Cloud.define("checkForLocationAndCreateIfDoesNotExist", function(request, response) {

  if (!request.user) {
    return response.error('not authorized');
  }
  
  var Location = Parse.Object.extend("Location");
  var query = new Parse.Query(Location);
     
  console.log("Here #1"); 
  query.equalTo("primary", request.params.primary);
  if (request.params.secondary){

    query.equalTo("secondary", request.params.secondary);

    if (request.params.tertiary){

      query.equalTo("tertiary", request.params.tertiary);

    }

  }
  console.log("Here #2");
     
  query.first({
    useMasterKey: true,
    success : function(object) {
 
        if(object){
 
          console.log("Location already in parse");
        //response.error(object);
        response.success(object.id);
 
      }else {
 
        var theLocation = new Location();
        theLocation.set("active", true);
        console.log("Here #3");
        theLocation.set("primary", request.params.primary);
        if (request.params.secondary){

          theLocation.set("secondary", request.params.secondary);

          if (request.params.tertiary) {

              theLocation.set("tertiary",  request.params.tertiary);
          }
        }
        console.log("Here #4");
 
         theLocation.save(null, {
        useMasterKey: true,
        success: function(theLocation) {
           // Execute any logic that should take place after the object is saved.
           //response.success('New object created with objectId: ' + theLocation.id);
            response.success(theLocation.id);
         },   
        error: function(theLocation, error) {
           // Execute any logic that should take place if the save fails.
           // error is a Parse.Error with an error code and description.
           response.error('Failed to create new object, with error code: ' + error.description);
         }
      });
 
      }
    },
    error : function(error) {
 
      // The user shouldn't see this error.
      console.error("[Location] beforeSave() error in query for Location name uniqueness");
      // But let's make it informative anyways
      response.error("There was a problem verifying the uniqueness of the Location.");
 
    }
  });
 
});  


Parse.Cloud.beforeSave("Asset", function (request,response) {

  console.log("Inside [Asset] beforeSave()");

  console.log(request);

//response.success();

  if (!request.user && !request.master) {
    return response.error('not authorized');
  }
  
  var latentAsset = request.object;
  var Asset = Parse.Object.extend("Asset");
    
  var history = [];
    
  if (latentAsset.id) {

    var query = new Parse.Query(Asset);
    query.get(latentAsset.id, {
      useMasterKey: true,
      success: function(asset) {
        // The object was retrieved successfully.
        var oldStatus = asset.get("status");
        var newStatus = latentAsset.get("status");
          
        if ( oldStatus != newStatus ) {
          console.log("[Asset] beforeSave() detected status change from " + oldStatus + " to " + newStatus );
          history = latentAsset.get("history");
          if (!history) {
            history = [];
          }
          var record = {
            action: "",
            actionDate: "",
            status: "",
            user: {
              name: "",
              objectid: "",
            }
          }
          record.action = "Status change";
          record.actionDate = new Date();
          record.status = latentAsset.get("status");
          record.user = {
            name: request.user.get("username"),
            objectid: request.user.id
          }
          history.push(record);
          latentAsset.set("history", history);
          
        }

        response.success();

      },
      error: function(object, error) {
        console.log("[Asset] beforeSave() detected new Asset (error getting existing)");
        history = [];
        var record = {
          action: "",
          actionDate: "",
          status: "",
          user: {
            name: "",
            objectid: "",
          }
       }
        record.action = "Asset created";
        record.actionDate = new Date();
        record.status = "READY";
        record.user = {
          name: request.user.get("username"),
          objectid: request.user.id
        }
        history.push(record);
        latentAsset.set("history", history);
        response.success();
        // The object was not retrieved successfully.
        // error is a Parse.Error with an error code and description.
      }
    });
  } else {
    console.log("[Asset] beforeSave() detected new Asset (no id on request)");
    var record = {
      action: "",
      actionDate: "",
      status: "",
      user: {
        name: "",
        objectid: "",
      }
    }
    record.action = "Asset created";
    record.actionDate = new Date();
    record.status = "READY";
    record.user = {
      name: request.user.get("username"),
      objectid: request.user.id
    }
    history.push(record);
    latentAsset.set("history", history);
    response.success();
  }
  
});
Parse.Cloud.beforeSave("AssetCategoryFacilities", function (request,response) {

  if (!request.user) {
    return response.error('not authorized');
  }

  console.log("Inside [AssetCategoryFacilities] beforeSave()");

  var latentAssetCategoryFacilities = request.object;
  var AssetCategoryFacilities = Parse.Object.extend("AssetCategoryFacilities");
    
  if (latentAssetCategoryFacilities.id) {

    var query = new Parse.Query("AssetCategoryFacilities");
    query.get(latentAssetCategoryFacilities.id, {
            useMasterKey: true,
            success: function(assetCategoryFacilities) {

              console.log("latentAssetCategoryFacilities was retrieved successfully!");

              var oldAssignedTo = assetCategoryFacilities.get("assignedTo");
              var newAssignedTo = latentAssetCategoryFacilities.get("assignedTo");

              if (newAssignedTo && (!oldAssignedTo || (oldAssignedTo.id != newAssignedTo.id))) {

                  console.log("[AssetCategoryFacilities] beforeSave() detected Assigned To change");

                  var masterAssetID = latentAssetCategoryFacilities.get("masterAssetID");

                   var query = new Parse.Query("Asset");
                   query.equalTo("objectId", masterAssetID.id);
                   query.first({
                          useMasterKey: true,
                          success : function(masterAsset) {

                              console.log("[AssetCategoryFacilities] beforeSave() success block for getting master asset.");

                              var query = new Parse.Query(Parse.User);
                              query.equalTo("objectId", newAssignedTo.id);
                              query.include("userInfo");
                              query.first({
                                  useMasterKey: true,
                                  success : function(object) {

                                      var history = [];
                                      history = masterAsset.get("history");
                                      if (!history) {
                                        history = [];
                                      }
                                      var record = {
                                        action: "",
                                        actionDate: "",
                                        status: "",
                                        user: {
                                          name: "",
                                          objectid: "",
                                        }
                                      }
                                      record.action = "Assigned To change";
                                      record.actionDate = new Date();

                                      var message="New Assigned To Name: " + object.get("userInfo").attributes.firstName + " " + object.get("userInfo").attributes.lastName;

                                      //console.log(message);
                
                                      record.status = message;
                                      record.user = {
                                          name: request.user.get("username"),
                                          objectid: request.user.id
                                      }
                                      history.push(record);
                                      masterAsset.set("history", history);

                                      //console.log(history);
                                      //console.log(object);

                                      masterAsset.save(null, {
                                            useMasterKey: true,
                                            success: function(theMasterAsset) {
                                              console.log("[AssetCategoryFacilities] beforeSave() successfully saved theMasterAsset!!");
                                              response.success();
                                            },   
                                            error: function(theMasterAsset, error) {
                                              console.log('[AssetCategoryFacilities] beforeSave() failed to save theMasterAsset, with error : ');
                                              console.log(error);
                                              response.success();
                                            }
                                      });

                                  }, // end success block: call: var query = new Parse.Query(Parse.User);
                                  error: function() {

                                      var history = [];
                                        history = masterAsset.get("history");
                                        if (!history) {
                                          history = [];
                                        }
                                      var record = {
                                        action: "",
                                        actionDate: "",
                                        status: "",
                                        user: {
                                          name: "",
                                          objectid: "",
                                        }
                                      }
                                      record.action = "Assigned To change";
                                      record.actionDate = new Date();

                                      var message="New Assigned To Name Unavailable";

                                      //console.log(history);

                                      history.push(record);
                                      masterAsset.set("history", history);

                                      //console.log(history);
                                      //console.log(object);

                                      masterAsset.save(null, {
                                            useMasterKey: true,
                                            success: function(theMasterAsset) {
                                              console.log("[AssetCategoryFacilities] beforeSave() successfully saved theMasterAsset!!");
                                              response.success();
                                            },   
                                            error: function(theMasterAsset, error) {
                                              console.log('[AssetCategoryFacilities] beforeSave() failed to save theMasterAsset, with error : ');
                                              console.log(error);
                                              response.success();
                                            }
                                      });
                                  } // end error block: call: var query = new Parse.Query(Parse.User);
                              });

                          }, // end success block: call: var query = new Parse.Query("Asset");
                          error: function() {
                                console.log("[AssetCategoryFacilities] beforeSave() error block for getting master asset");
                                response.success();

                          } //end error block: call: var query = new Parse.Query("Asset");
                  });

              } else { //if (newAssignedTo && (!oldAssignedTo || (oldAssignedTo.id != newAssignedTo.id))) {

                response.success();
              }

            }, // end success block: call: query.get(latentAssetCategoryFacilities.id, {
            error: function() {

               response.success();

            } // end error block: call: query.get(latentAssetCategoryFacilities.id, {

    });//end call: query.get(latentAssetCategoryFacilities.id, {

  } else { //if (latentAssetCategoryFacilities.id) {

      response.success();
  }

});