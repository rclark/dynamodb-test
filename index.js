var crypto = require('crypto');
var AWS = require('aws-sdk');
var _ = require('underscore');
var stream = require('stream');
var Dyno = require('@mapbox/dyno');
var dynalite = require('dynalite')({
  createTableMs: 0,
  updateTableMs: 0,
  deleteTableMs: 0
});

var listening = false;

module.exports = ddbtest;
module.exports.fixedName = function(test, tableName, tableDef) {
  var dynamo = ddbtest(test, tableName, tableDef);
  dynamo.tableName = dynamo.tableDef.TableName = tableName;
  return dynamo;
};

function ddbtest(test, projectName, tableDef, region) {
  var live = !!region;
  tableDef = _(tableDef).clone();

  function getKeys(item) {
    var keyNames = tableDef.KeySchema.map(function(key) {
      return key.AttributeName;
    });

    return keyNames.reduce(function(key, name) {
      key[name] = item[name];
      return key;
    }, {});
  }

  var dynamodb = {};

  dynamodb.tableName = tableDef.TableName = [
    'test',
    projectName,
    crypto.randomBytes(4).toString('hex')
  ].join('-');

  dynamodb.tableDef = tableDef;

  var options = dynamodb.config = live ? { region: region } : {
    region: 'fake',
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    endpoint: 'http://localhost:4567'
  };

  dynamodb.dynamo = new AWS.DynamoDB(options);
  dynamodb.dyno = Dyno(_({ table: dynamodb.tableName }).extend(options));

  var tableRunning = false;

  function start(assert, callback) {
    if (live) assert.timeoutAfter(300000);
    if (tableRunning) return assert.end();

    function done(err) {
      if (err) throw err;
      tableRunning = true;
      callback();
    }

    if (live) return dynamodb.dyno.createTable(tableDef, done);
    if (listening) return dynamodb.dyno.createTable(tableDef, done);

    dynalite.listen(4567, function(err) {
      if (err) throw err;
      listening = true;
      dynamodb.dyno.createTable(tableDef, done);
    });
  }

  dynamodb.start = function() {
    test('[dynamodb-test] create table', function(assert) {
      start(assert, function() {
        assert.end();
      });
    });
  };

  dynamodb.delete = function() {
    test('[dynamodb-test] delete table', function(assert) {
      if (live) assert.timeoutAfter(300000);
      if (!tableRunning) return assert.end();

      dynamodb.dyno.deleteTable({
        TableName: dynamodb.tableName
      }, function(err) {
        if (err) throw err;
        tableRunning = false;
        assert.end();
      });
    });
  };

  dynamodb.load = function(fixtures) {
    test('[dynamodb-test] load fixtures', function(assert) {
      if (!tableRunning) start(assert, load);
      else load();

      function load() {
        if (live) assert.timeoutAfter(300000);

        var params = { RequestItems: {} };
        params.RequestItems[dynamodb.tableName] = fixtures.map(function(item) {
          return { PutRequest: { Item: item } };
        });

        var requestSet = dynamodb.dyno.batchWriteItemRequests(params);

        var attempts = 0;
        (function write(requestSet) {
          requestSet.sendAll(10, function(err, responses, unprocessed) {
            if (err) throw err;
            attempts++;

            if (unprocessed && unprocessed.length)
              return setTimeout(write, Math.pow(2, attempts), unprocessed);

            assert.end();
          });
        })(requestSet);
      }
    });
  };

  dynamodb.empty = function() {
    test('[dynamodb-test] empty table', function(assert) {
      if (!tableRunning) start(assert, empty);
      else empty();

      function empty() {
        if (live) {
          assert.timeoutAfter(300000);

          var deletes = new stream.Writable({ objectMode: true });
          deletes.pending = false;
          var keys = [];
          keys.send = function(callback) {
            var params = { RequestItems: {} };
            params.RequestItems[dynamodb.tableName] = keys.splice(0, 25).map(function(key) {
              return { DeleteRequest: { Key: key } };
            });

            (function destroy(params) {
              deletes.pending = true;
              dynamodb.dyno.batchWriteItem(params, function(err, data) {
                deletes.pending = false;
                if (err) throw err;

                if (data.UnprocessedItems && Object.keys(data.UnprocessedItems).length)
                  return destroy({RequestItems: data.UnprocessedItems });

                callback();
              });
            })(params);
          };

          deletes._write = function(item, enc, callback) {
            keys.push(getKeys(item));
            if (keys.length < 25) return callback();
            keys.send(callback);
          };

          var end = deletes.end.bind(deletes);
          deletes.end = function() {
            if (deletes.pending) return setImmediate(deletes.end);
            if (!keys.length) return end();
            keys.send(end);
          };

          dynamodb.dyno.scanStream({ ConsistentRead: true })
            .pipe(deletes)
            .on('finish', function() {
              assert.end();
            });

          return;
        }

        dynamodb.dyno.deleteTable(dynamodb.tableName, function(err) {
          if (err) throw err;
          tableRunning = false;
          dynamodb.dyno.createTable(dynamodb.tableDef, function(err) {
            if (err) throw err;
            tableRunning = true;
            assert.end();
          });
        });
      }
    });
  };

  dynamodb.test = function(name, fixtures, callback) {
    dynamodb.empty();

    if (typeof fixtures === 'function') {
      callback = fixtures;
      fixtures = null;
    }

    if (fixtures && fixtures.length) dynamodb.load(fixtures);

    test(name, callback);
    dynamodb.empty();
  };

  if (!live) dynamodb.close = function() {
    test('[dynamodb-test] close dynalite', function(assert) {
      dynamodb.dyno.deleteTable(dynamodb.tableName, function(err) {
        if (err) throw err;
        dynalite.close(function(err) {
          if (err) throw err;
          listening = false;
          assert.end();
        });
      });
    });
  };

  return dynamodb;
}
