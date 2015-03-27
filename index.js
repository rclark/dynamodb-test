var test = require('tape');
var crypto = require('crypto');
var AWS = require('aws-sdk');
var _ = require('underscore');
var Dyno = require('dyno');
var dynalite = require('dynalite')({
  createTableMs: 0,
  updateTableMs: 0,
  deleteTableMs: 0
});

module.exports = function(projectName, tableDef, region) {
  var live = !!region;
  tableDef = _(tableDef).clone();

  var dynamodb = {};

  dynamodb.tableName = tableDef.TableName = [
    'test',
    projectName,
    crypto.randomBytes(4).toString('hex')
  ].join('-');

  dynamodb.tableDef = tableDef;

  var options = live ? { region: region } : {
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

    dynalite.listen(4567, function(err) {
      if (err) throw err;
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

      dynamodb.dyno.deleteTable(dynamodb.tableName, function(err) {
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

        dynamodb.dyno.putItems(fixtures, function(err) {
          if (err) throw err;
          assert.end();
        });
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
          return dynamodb.dyno.scan(function(err, items) {
            if (err) throw err;
            dynamodb.dyno.deleteItems(items, function(err) {
              if (err) throw err;
              assert.end();
            });
          });
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
      dynalite.close(function(err) {
        if (err) throw err;
        assert.end();
      });
    });
  };

  return dynamodb;
};
