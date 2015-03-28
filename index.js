var express = require('express');
var watch = require('watch');
var probe = require('node-ffprobe');
var util = require("util");

var schedule = require('node-schedule');

var mkdirp = require('mkdirp');
var app = express(),
  http = require('http'),
  server = http.createServer(app);
var WebSocketServer = require('websocket').server;


var csv = require('csv');
var sys = require('sys');
var fs = require('fs');
var path = require('path');
var config = require('./config.json');
var mongo = require('mongodb');
var BSON = mongo.BSONPure;
var MongoClient = require('mongodb').MongoClient
  , assert = require('assert');

var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');


// Connection URL
var url = 'mongodb://localhost:27017/scanner';


//var host = process.env['MONGO_NODE_DRIVER_HOST'] != null ? process.env['MONGO_NODE_DRIVER_HOST'] : 'localhost';
//var port = process.env['MONGO_NODE_DRIVER_PORT'] != null ? process.env['MONGO_NODE_DRIVER_PORT'] : Connection.DEFAULT_PORT;
//var scanner = new Db('scanner', new Server(host, port, {}));
var db;
var channels = {};
var clients = [];
var stats = {};
var affiliations = {};
var sources = {};
var source_names = {};



//io.set('log level', 1);
MongoClient.connect(url, function(err, scannerDb) {
  assert.equal(null, err);
  console.log("Connected correctly to server");
  db = scannerDb;
  scannerDb.authenticate(config.dbUser, config.dbPass, function() {
    //do the initial build of the stats
      db.collection("system.namespaces").find({name: "scanner.transmissions"}).toArray(
        function(err, docs) {
            assert.equal(err, null);
            if(docs.length) {
                console.log("Collection exists!");
            } else {
                console.log("Collection doesn't exist :(");
            }
      });
    db.collection('call_volume', function(err, collection) {
      build_stat(collection);
    });
    db.collection('source_names', function(err, collection){
      collection.find().toArray(function(err, results) {
        for (var src in results) {
          source_names[results[src]._id] = { name: results[src].name, shortName: results[src].shortName};
        }

      });
    });
    build_unit_affiliation();
    db.collection('source_list', function(err, collection) {

    collection.find( function(err, cursor) {
      cursor.sort({"value.total":-1}).toArray(function(err, results) {
      sources = results;
    });
    });

    });
  });
});


/*
scanner.open(function(err, scannerDb) {
  db = scannerDb;
  scannerDb.authenticate(config.dbUser, config.dbPass, function() {
    //do the initial build of the stats
    db.collection('call_volume', function(err, collection) {
      build_stat(collection);
    });
    db.collection('source_names', function(err, collection){
      collection.find().toArray(function(err, results) {
        for (var src in results) {
          source_names[results[src]._id] = { name: results[src].name, shortName: results[src].shortName};
        }

      });
    });
    build_unit_affiliation();
    db.collection('source_list', function(err, collection) {

    collection.find( function(err, cursor) {
      cursor.sort({"value.total":-1}).toArray(function(err, results) {
      sources = results;
    });
    });

    });
  });
});
*/

var numResults = 50;
var talkgroup_filters = {};

/*
fs.createReadStream('ChanList.csv').pipe(csv.parse({columns: [ 'Num', 'Hex', 'Mode', 'Alpha', 'Description', 'Tag', 'Group' ]})).pipe(csv.transform(function(row) {
    console.log(row);
        channels[row.Num] = {
      alpha: row.Alpha,
      desc: row.Description,
      tag: row.Tag,
      group: row.Group
    };
    var tg_array = new Array();
    tg_array.push(parseInt(row.Num));
    talkgroup_filters['tg-' + row.Num] = tg_array;

    var tag_key = 'group-' + row.Group.toLowerCase();
    if (!(tag_key in talkgroup_filters)) {
      talkgroup_filters[tag_key] = new Array();
    }
    talkgroup_filters[tag_key].push(parseInt(row.Num));

    return row;
    // handle each row before the "end" or "error" stuff happens above
})).on('readable', function(){
  while(this.read()){}
}).on('end', function() {
    // yay, end
}).on('error', function(error) {
    // oh no, error
});
*/


function build_affiliation_array(collection) {
  affiliations = {};
  collection.find().toArray(function(err, results) {
      if (err) console.log(err);
      if (results && (results.length > 0)) {
        for (var i = 0; i < results.length; i++) {
          console.log(util.inspect(results[i]));
          affiliations[results[i]._id.tg] = results[i].value.unit_count;
        }
      }
      console.log(util.inspect(affiliations));
  });

}


function build_stat(collection) {
  var chan_count = 0;
  stats = {};
  var db_count = 0;
  for (var chan_num in channels) {
    var historic = new Array();
    chan_count++;

    for (hour = 0; hour < 24; hour++) {

      historic[hour] = 0;
    }
    stats[chan_num] = {
      name: channels[chan_num].alpha,
      desc: channels[chan_num].desc,
      num: chan_num,
      historic: historic
    };
    var query = {
      "_id.talkgroup": parseInt(chan_num)
    };
    collection.find(query).toArray(function(err, results) {
      db_count++;
      if (err) console.log(err);
      if (results && (results.length > 0)) {
        for (var i = 0; i < results.length; i++) {
          stats[results[0]._id.talkgroup].historic[results[i]._id.hour] = results[i].value.count;
        }
      }
      if (chan_count == db_count) {
        for (var chan_num in stats) {
          var chan = stats[chan_num];
          var erase_me = true;
          for (var i = 0; i < chan.historic.length; i++) {
            if (chan.historic[i] != 0) {
              erase_me = false;
              break;
            }
          }
          if (erase_me) {
            delete stats[chan_num];
          }
        }

      }
    });

  }
}

function build_unit_affiliation() {
    map = function() {
    var now = new Date();
    var difference = now.getTime() - this.date.getTime();
    var minute = Math.floor(difference / 1000 / 60 / 5);
    emit({
      tg: this.tg
    }, {
      count: this.count,
      minute: minute

    });
  }

  reduce = function(key, values) {
  var result = {
    unit_count: []
  };
values.forEach(function(v){
        result.unit_count[v.minute] = v.count;
});
return result;
  }


db.collection('affiliation', function(err, afilCollection) {
    var now = new Date();
    afilCollection.mapReduce(map, reduce, {
      query: {
          date: { // 18 minutes ago (from now)
              $gt: new Date(now.getTime() - 1000 * 60 * 60)
          }
      },
      out: {
        replace: "recent_affiliation"
      }
    }, function(err, collection) {
      if (err) console.error(err);
      if (collection) {
        build_affiliation_array(collection);
      }
    });
  });

}
function build_call_volume() {

  map = function() {
    var now = new Date();
    var difference = now.getTime() - this.time.getTime();
    var hour = Math.floor(difference / 1000 / 60 / 60);
    emit({
      hour: hour,
      talkgroup: this.talkgroup
    }, {
      count: 1
    });
  }

  reduce = function(key, values) {
    var count = 0;

    values.forEach(function(v) {
      count += v['count'];
    });

    return {
      count: count
    };
  }
  db.collection('transmissions', function(err, transCollection) {
    var yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    transCollection.mapReduce(map, reduce, {
      query: {
        time: {
          $gte: yesterday
        }
      },
      out: {
        replace: "call_volume"
      }
    }, function(err, collection) {
      if (err) console.error(err);
      if (collection) {
        build_stat(collection);
      }
    });
  });
}

function build_source_list() {
  map = function() {
    if (this.srcList) {
    for (var idx = 0; idx < this.srcList.length; idx++) {
        var key = this.srcList[idx];
        var value = {};
        value[this.talkgroup] = 1;

        emit(key, value);
    }
    }
}

finalize = function(key, values) {
    var count=0;
    for(var k in values)
    {
        count += values[k];
    }

    values['total'] = count;
    return values;
}

reduce = function(key, values) {
    var talkgroups = {};



    values.forEach(function(v) {
        for(var k in v) { // iterate colors
            if(!talkgroups[k]) // init missing counter
            {
                talkgroups[k] = 0;
            }
            talkgroups[k] += v[k];

        }

    });



    return talkgroups;
}


  db.collection('transmissions', function(err, transCollection) {
    var yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    transCollection.mapReduce(map, reduce, {
      query: {
        time: {
          $gte: yesterday
        }
      },
      out: {
        replace: "source_list"
      },
      finalize: finalize
    }, function(err, collection) {
      if (err) console.error(err);
      if (collection) {
        //build_stat(collection);
      }
    });
  });
}


















app.get('/channels', function(req, res) {

  res.contentType('json');
  res.send(JSON.stringify({
    channels: channels,
    source_names: source_names
  }));

});

app.get('/card/:id', function(req, res) {
  var objectId = req.params.id;
  var o_id = new BSON.ObjectID(objectId);
  db.collection('transmissions', function(err, transCollection) {
    transCollection.findOne({
        '_id': o_id
      },
      function(err, item) {
        //console.log(util.inspect(item));
        if (item) {
          var time = new Date(item.time);
          var timeString = time.toLocaleTimeString("en-US");
          var dateString = time.toDateString();
          res.render('card', {
            item: item,
            channel: channels[item.talkgroup],
            time: timeString,
            date: dateString
          });

        } else {
          res.send(404, 'Sorry, we cannot find that!');
        }
      });
  });
});


app.get('/star/:id', function(req, res) {
  var objectId = req.params.id;
  var o_id = new BSON.ObjectID(objectId);
  db.collection('transmissions', function(err, transCollection) {
    transCollection.findAndModify({'_id': o_id}, [],
     {$inc: {stars: 1}}, {new: true},
    function(err, object) {

      if (err){
        console.warn(err.message); // returns error if no matching object found
      }else{
            res.contentType('json');
            res.send(JSON.stringify({
              stars: object.stars
            }));
      }

  });
  });
});



app.get('/call/:id', function(req, res) {
  var objectId = req.params.id;
  var o_id = new BSON.ObjectID(objectId);
  db.collection('transmissions', function(err, transCollection) {
    transCollection.findOne({
        '_id': o_id
      },
      function(err, item) {
        //console.log(util.inspect(item));
        if (item) {
          var time = new Date(item.time);
          var timeString = time.toLocaleTimeString("en-US");
          var dateString = time.toDateString();

          res.render('call', {
            item: item,
            channel: channels[item.talkgroup],
            talkgroup: item.talkgroup,
            time: timeString,
            date: dateString,
            objectId: objectId,
            freq: item.freq,
            srcList: item.srcList,
            audioErrors: item.audioErrors,
            headerErrors: item.headerErrors,
            headerCriticalErrors: item.headerCriticalErrors,
            symbCount: item.symbCount,
            recNum: item.recNum
          });

        } else {
          res.send(404, 'Sorry, we cannot find that!');
        }
      });
  });
});

function get_calls(query, res) {

  var calls = [];
  console.log(util.inspect(query.filter));
  db.collection('transmissions', function(err, transCollection) {
    transCollection.find(query.filter).count(function(e, count) {
      transCollection.find(query.filter, function(err, cursor) {
        cursor.sort(query.sort_order).limit(numResults).each(function(err, item) {
          if (item) {
            call = {
              objectId: item._id,
              talkgroup: item.talkgroup,
              filename: item.path + item.name,
              time: item.time,
              freq: item.freq,
              srcList: item.srcList,
              stars: item.stars,
              len: Math.round(item.len)
            };
            calls.push(call);
          } else {
            res.contentType('json');
            res.send(JSON.stringify({
              calls: calls,
              count: count,
              direction: query.direction
            }));
          }
        });
      });
    });
  });


}

function build_filter(code, start_time, direction, stars) {
  var filter = {};

  if (code) {
    if (code.substring(0, 3) == 'tg-') {
      tg_num = parseInt(code.substring(3));
      filter = {
        talkgroup: tg_num
      };
    } else if (code.substring(0, 4) == 'src-') {
      src_num = parseInt(code.substring(4));
      filter = {
        srcList: src_num
      };
    } else if (code.substring(0, 6) == 'group-') {
          filter = {
            talkgroup: {
              $in: talkgroup_filters[code]
            }
          };
    } else {
      switch (code) {
        case 'tag-ops':
        case 'tag-ems':
        case 'tag-fire-dispatch':
        case 'tag-fire':
        case 'tag-hospital':
        case 'tag-interop':
        case 'tag-law-dispatch':
        case 'tag-public-works':
        case 'tag-public-health':
        case 'tag-paratransit':
        case 'tag-st-e':
        case 'tag-water':
        case 'tag-parks':
        case 'tag-parking':
        case 'tag-security':
         case 'tag-transportation':
         case 'tag-water':
          filter = {
            talkgroup: {
              $in: talkgroup_filters[code]
            }
          };
          break;
      }
    }
  }

  if (start_time) {
    var start = new Date(start_time);
    if (direction == 'newer') {
      filter.time = {
        $gt: start
      };
    } else {
      filter.time = {
        $lt: start
      };
    }

  }
  filter.len = {
    $gte: 1.0
  };

  if (stars) {
    filter.stars = { $gt: 0};
  }
  var sort_order = {};
  if (direction == 'newer') {
    sort_order['time'] = 1;
  } else {
    sort_order['time'] = -1;
  }

  var query = {};
  query['filter'] = filter;
  query['direction'] = direction;
  query['sort_order'] = sort_order;

  return query;
}

app.get('/calls/newer/:time', function(req, res) {
  var start_time = parseInt(req.params.time);
  var query = build_filter(null, start_time, 'newer', false);

  get_calls(query, res);
});

app.get('/calls/newer/:time/:filter_code', function(req, res) {
  var filter_code = req.params.filter_code;
  var start_time = parseInt(req.params.time);
  var query = build_filter(filter_code, start_time, 'newer', false);

  get_calls(query, res);
});

app.get('/calls/older/:time', function(req, res) {

  var start_time = parseInt(req.params.time);
  console.log("time: " + start_time );
  console.log(util.inspect(req.params));
  var query = build_filter(null, start_time, 'older', false);

  get_calls(query, res);
});
app.get('/calls/older/:time/:filter_code', function(req, res) {
  var filter_code = req.params.filter_code;
  var start_time = parseInt(req.params.time);
  console.log("time: " + start_time + " Filter code: " + filter_code);
  console.log(util.inspect(req.params));
  var query = build_filter(filter_code, start_time, 'older', false);

  get_calls(query, res);
});

app.get('/calls', function(req, res) {
  var filter_code = req.params.filter_code;
  var query = build_filter(null, null, 'older', false);

  get_calls(query, res);
});

app.get('/calls/:filter_code', function(req, res) {
  var filter_code = req.params.filter_code;
  var query = build_filter(filter_code, null, 'older', false);

  get_calls(query, res);
});

app.get('/stars/newer/:time/:filter_code?*', function(req, res) {
  var filter_code = req.params.filter_code;
  var start_time = parseInt(req.params.time);
  var query = build_filter(filter_code, start_time, 'newer', true);

  get_calls(query, res);
});

app.get('/stars/older/:time/:filter_code?*', function(req, res) {
  var filter_code = req.params.filter_code;
  var start_time = parseInt(req.params.time);
  var query = build_filter(filter_code, start_time, 'older', true);

  get_calls(query, res);
});

app.get('/stars/:filter_code?*', function(req, res) {
  var filter_code = req.params.filter_code;
  var query = build_filter(filter_code, null, 'older', true);

  get_calls(query, res);
});




app.get('/volume', function(req, res) {
  res.contentType('json');
  res.send(JSON.stringify(stats));
});
app.get('/affiliation', function(req, res) {
  res.contentType('json');
  res.send(JSON.stringify(affiliations));
});
app.get('/source_list', function(req, res) {
  res.contentType('json');
  res.send(JSON.stringify(sources));
});


function notify_clients(call) {
  call.type = "calls";
  console.log("New Call sending to " + clients.length + " clients");
  for (var i = 0; i < clients.length; i++) {
    //console.log(util.inspect(clients[i].socket));
    if (clients[i].code == "") {
      //console.log("Call TG # is set to All");
      console.log(" - Sending one");
      clients[i].socket.send(JSON.stringify(call));
    } else {
      if (typeof talkgroup_filters[clients[i].code] !== "undefined") {
        //console.log("Talkgroup filter found: " + clients[i].code);

        if (talkgroup_filters[clients[i].code].indexOf(call.talkgroup) > -1) {
          //console.log("Call TG # Found in filer");
          console.log(" - Sending one filter");
          clients[i].socket.send(JSON.stringify(call));
        }
      }
    }
  }
}
watch.createMonitor(config.uploadDir, function(monitor) {
  monitor.files['*.m4a'];



  monitor.on("created", function(f, stat) {

    if ((path.extname(f) == '.json') && (monitor.files[f] === undefined))  {
      var name = path.basename(f, '.json');
      var regex = /([0-9]*)-unit_check/
      var result = name.match(regex);
      if (result!=null)
      {
        console.log("Unit Check: " + f);
        fs.readFile(f, 'utf8', function (err, data) {
          console.log("Error: " + err);

            if (!err) {
              try {
              data = JSON.parse(data);
              } catch (e) {
                // An error has occured, handle it, by e.g. logging it
                data.talkgroups = {};
                console.log(e);
              }
              console.log("Data: " + util.inspect(data));
              db.collection('affiliation', function(err, affilCollection) {

                for (talkgroup in data.talkgroups) {
                  var affilItem = {
                    tg: talkgroup,
                    count: data.talkgroups[talkgroup],
                    date: new Date()
                  };

                    affilCollection.insert(affilItem, function(err, objects) {
                      if (err) console.warn(err.message);

                    });
                }
              });

            }
        });
      }
    }
    if ((path.extname(f) == '.m4a') && (monitor.files[f] === undefined)) {
      var name = path.basename(f, '.m4a');

      var regex = /([0-9]*)-([0-9]*)_([0-9.]*)/
      var result = name.match(regex);
      //console.log(name);
      //console.log(util.inspect(result));
      if (result!=null)
      {
        var tg = parseInt(result[1]);
        var time = new Date(parseInt(result[2]) * 1000);
        var freq = parseFloat(result[3]);
        var base_path = config.mediaDir;
        var local_path = "/" + time.getFullYear() + "/" + time.getMonth() + "/" + time.getDate() + "/";
        mkdirp.sync(base_path + local_path, function(err) {
          if (err) console.error(err);
        });
        var target_file = base_path + local_path + path.basename(f);
        var json_file = path.dirname(f) + "/" + name + ".json";

        fs.readFile(json_file, 'utf8', function (err, data) {
          if (err) {
            console.log('JSON Error: ' + err);
                          console.log("Base: " + base_path + " Local: " + local_path + " Basename: " + path.basename(f));
              console.log("F Path: " + path.dirname(f));
                var srcList = [];
                var headerCriticalErrors = 0;
                var headerErrors = 0;
                var audioErrors = 0;
                var symbCount = 0;
                var srcList = 0;
                var recNum = 0;
          } else {
                 data = JSON.parse(data);
                var srcList = data.srcList;
                var headerCriticalErrors = data.headerCriticalErrors;
                var headerErrors = data.headerErrors;
                var audioErrors = data.audioErrors;
                var symbCount = data.symbCount;
                var srcList = data.srcList;
                var recNum = data.num;
                fs.unlink(json_file, function (err) {
                if (err)
                console.log('JSON Delete Error: ' + err + " JSON: " + json_file);
              });
          }


          fs.rename(f, target_file, function(err) {
            if (err) {
              console.log("Rename Error: " + err);
              console.log("Base: " + base_path + " Local: " + local_path + " Basename: " + path.basename(f));
              console.log("F Path: " + path.dirname(f));
              //throw err;

            } else {

            probe(target_file, function(err, probeData) {

              transItem = {
                talkgroup: tg,
                time: time,
                name: path.basename(f),
                freq: freq,
                stars: 0,
                path: local_path,
                srcList: srcList,
                headerCriticalErrors: headerCriticalErrors,
                headerErrors: headerErrors,
                audioErrors: audioErrors,
                symbCount: symbCount,
                srcList: srcList,
                recNum: recNum
              };
              //transItem.len = reader.chunkSize / reader.byteRate;

              if (err) {
                //console.log("Error with FFProbe: " + err);
                transItem.len = -1;
              } else {
                transItem.len = probeData.format.duration;
              }
              db.collection('transmissions', function(err, transCollection) {
                transCollection.insert(transItem, function(err, objects) {
                  if (err) console.warn(err.message);
                  var objectId = transItem._id;

                  //console.log("Added: " + f);
                  var call = {
                    objectId: objectId,
                    talkgroup: transItem.talkgroup,
                    filename: transItem.path + transItem.name,
                    stars: transItem.stars,
                    freq: transItem.freq,
                    time: transItem.time,
                    len: Math.round(transItem.len)
                  };

                  // we only want to notify clients if the clip is longer than 1 second.
                  if (transItem.len >= 1) {
                    notify_clients(call);
                  }
                });
              });
           });
            }
          });

        });

      }
    }
  });
});

wsServer = new WebSocketServer({
    httpServer: server,
    // You should not use autoAcceptConnections for production
    // applications, as it defeats all standard cross-origin protection
    // facilities built into the protocol and the browser.  You should
    // *always* verify the connection's origin and decide whether or not
    // to accept it.
    autoAcceptConnections: false
});

function originIsAllowed(origin) {
  // put logic here to detect whether the specified origin is allowed.
  return true;
}

wsServer.on('request', function(request) {
    if (!originIsAllowed(request.origin)) {
      // Make sure we only accept requests from an allowed origin
      request.reject();
      console.log(('Rejected: ' + new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
      return;
    } else {
      console.log(('Accepted: ' + new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
    }

    var connection = request.accept(null, request.origin);
    var client = {
      socket: connection,
      code: null
    };
    clients.push(client);
    console.log((new Date()) + ' Connection accepted.');
    connection.on('message', function(message) {
        if (message.type === 'utf8') {
          var data = JSON.parse(message.utf8Data);
          if (typeof data.type !== "undefined") {
            if (data.type == 'code') {
              var index = clients.indexOf(client);
              if (index != -1) {
                clients[index].code = data.code;
                console.log("Client " + index + " code set to: " + data.code);
              } else {
                console.log("Client not Found!");
              }
            }


          }
            console.log('Received Message: ' + message.utf8Data);
            connection.sendUTF(message.utf8Data);
        }
        else if (message.type === 'binary') {
            console.log('Received Binary Message of ' + message.binaryData.length + ' bytes');
            connection.sendBytes(message.binaryData);
        }
    });
    connection.on('close', function(reasonCode, description) {
        console.log((new Date()) + ' Peer ' + connection.remoteAddress + ' disconnected.');
        for(var i = 0; i < clients.length; i++) {
          // # Remove from our connections list so we don't send
          // # to a dead socket
          if(clients[i].socket == connection) {
            clients.splice(i);
            break;
          }
        }
    });
});



schedule.scheduleJob({
  minute: 0
}, function() {
  build_unit_affiliation();
});


schedule.scheduleJob({
  minute: new schedule.Range(0, 59, 5)
}, function() {
  build_call_volume();
});

schedule.scheduleJob({
  minute: 30,
  hour: 1
}, function() {
  build_source_list();
});


server.listen(3004);
