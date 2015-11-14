var test = require('tape');
var crypto = require('crypto');
var _ = require('underscore');

var project = crypto.randomBytes(4).toString('hex');
var tableDef = require('./table.json');

var mocked = require('..')(test, project, tableDef);
var live = require('..')(test, project, tableDef, 'us-east-1');

test('sets tableName', function(assert) {
  var re = new RegExp('test-' + project + '-[a-zA-Z0-9]{8}');
  assert.ok(re.test(mocked.tableName), 'mocked sets tableName');
  assert.ok(re.test(live.tableName), 'live sets tableName');
  assert.end();
});

test('sets tableDef', function(assert) {
  var re = new RegExp('test-' + project + '-[a-zA-Z0-9]{8}');
  var mockedDef = _({ TableName: 'test' }).defaults(mocked.tableDef);
  var liveDef = _({ TableName: 'test' }).defaults(live.tableDef);
  assert.ok(re.test(mocked.tableDef.TableName), 'mocked sets name in tabledef');
  assert.ok(re.test(live.tableDef.TableName), 'live sets name in tabledef');
  assert.deepEqual(mockedDef, tableDef, 'mocked provides expected tabledef');
  assert.deepEqual(liveDef, tableDef, 'live provides expected tabledef');
  assert.end();
});

test('provides sdks', function(assert) {
  assert.ok(mocked.dyno, 'provides dyno');
  assert.ok(mocked.dynamo, 'provides dynamo');
  assert.ok(live.dyno, 'provides dyno');
  assert.ok(live.dynamo, 'provides dynamo');
  assert.end();
});

mocked.start();

test('mocked start', function(assert) {
  mocked.dynamo.listTables({}, function(err, data) {
    if (err) throw err;
    assert.deepEqual(data.TableNames, [mocked.tableName], 'creates the table');
    assert.end();
  });
});

mocked.load([{id: 'hey', range: 1}]);

test('mocked fixture load', function(assert) {
  mocked.dyno.scan(function(err, items) {
    if (err) throw err;
    assert.deepEqual(items, { Count: 1, Items: [ { id: 'hey', range: 1 } ], ScannedCount: 1 }, 'loaded fixtures');
    assert.end();
  });
});

mocked.empty();

test('mocked empty', function(assert) {
  mocked.dyno.scan(function(err, items) {
    if (err) throw err;
    assert.deepEqual(items, { Count: 0, Items: [], ScannedCount: 0 }, 'emptied database');
    assert.end();
  });
});

var secondMock = require('..')(test, project, tableDef);
secondMock.start();

test('mock two tables', function(assert) {
  mocked.dynamo.listTables({}, function(err, data) {
    if (err) throw err;
    assert.equal(data.TableNames.length, 2, 'creates two tables');
    assert.ok(data.TableNames.indexOf(mocked.tableName) > -1, 'created first mock table');
    assert.ok(data.TableNames.indexOf(secondMock.tableName) > -1, 'created second mock table');
    assert.end();
  });
});

secondMock.delete();
mocked.delete();

test('mocked delete', function(assert) {
  mocked.dynamo.listTables({}, function(err, data) {
    if (err) throw err;
    assert.deepEqual(data.TableNames, [], 'deletes the table');
    assert.end();
  });
});

mocked.test('mocked test', function(assert) {
  assert.pass('runs the test');
  mocked.dynamo.listTables({}, function(err, data) {
    if (err) throw err;
    assert.deepEqual(data.TableNames, [mocked.tableName], 'creates the table');
    assert.end();
  });
});

test('mocked test cleanup', function(assert) {
  mocked.dyno.scan(function(err, items) {
    if (err) throw err;
    assert.deepEqual(items, { Count: 0, Items: [], ScannedCount: 0 }, 'emptied database');
    assert.end();
  });
});

mocked.test('mocked test with fixtures', [{id: 'hey', range: 1}], function(assert) {
  assert.pass('runs the test');
  mocked.dynamo.listTables({}, function(err, data) {
    if (err) throw err;
    assert.deepEqual(data.TableNames, [mocked.tableName], 'creates the table');
    mocked.dyno.scan(function(err, items) {
      if (err) throw err;
      assert.deepEqual(items, { Count: 1, Items: [ { id: 'hey', range: 1 } ], ScannedCount: 1 }, 'loaded fixtures');
      assert.end();
    });
  });
});

test('mocked test with fixtures cleanup', function(assert) {
  mocked.dyno.scan(function(err, items) {
    if (err) throw err;
    assert.deepEqual(items, { Count: 0, Items: [], ScannedCount: 0 }, 'emptied database');
    assert.end();
  });
});

mocked.close();

test('mocked close dynalite', function(assert) {
  mocked.dyno.scan(function(err) {
    assert.equal(err.errno, 'ECONNREFUSED', 'dynalite is closed');
    assert.end();
  });
});

live.start();

test('live start', function(assert) {
  live.dynamo.listTables({}, function(err, data) {
    if (err) throw err;
    assert.ok(data.TableNames.indexOf(live.tableName), 'creates the table');
    assert.end();
  });
});

live.load([{id: 'hey', range: 1}]);

test('live fixture load', function(assert) {
  live.dyno.scan(function(err, items) {
    if (err) throw err;
    assert.deepEqual(items, { Count: 1, Items: [ { id: 'hey', range: 1 } ], ScannedCount: 1 }, 'loaded fixtures');
    assert.end();
  });
});

live.empty();

test('live empty', function(assert) {
  live.dyno.scan(function(err, items) {
    if (err) throw err;
    assert.deepEqual(items, { Count: 0, Items: [], ScannedCount: 0 }, 'emptied database');
    assert.end();
  });
});

live.load(_.range(998).map(function(i) {
  return {
    id: crypto.randomBytes(16).toString('hex'),
    range: i,
    data: crypto.randomBytes(1200)
  };
}));

live.empty();

test('live empty multiple pages', function(assert) {
  live.dyno.scan({ ConsistentRead: true }, function(err, items) {
    if (err) throw err;
    assert.deepEqual(items, { Count: 0, Items: [], ScannedCount: 0 }, 'emptied database');
    assert.end();
  });
});

live.delete();

test('live delete', function(assert) {
  live.dynamo.listTables({}, function(err, data) {
    if (err) throw err;
    assert.equal(data.TableNames.indexOf(live.tableName), -1, 'deletes the table');
    assert.end();
  });
});

live.test('live test', function(assert) {
  assert.pass('runs the test');
  live.dynamo.listTables({}, function(err, data) {
    if (err) throw err;
    assert.ok(data.TableNames.indexOf(live.tableName), 'creates the table');
    assert.end();
  });
});

test('live test cleanup', function(assert) {
  live.dyno.scan(function(err, items) {
    if (err) throw err;
    assert.deepEqual(items, { Count: 0, Items: [], ScannedCount: 0 }, 'emptied database');
    assert.end();
  });
});

live.delete();
