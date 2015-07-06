# dynamodb-test

Create and destroy DynamoDB and Dynalite tables for use in [tape](https://github.com/substack/tape) tests

## Simple example

```js
var tape = require('tape');
var tableDef = require('./table-definition.json');
var dynamodb = require('dynamodb-test')(tape, 'my-tests', tableDef, 'us-east-1');

dynamodb.test('my test', function(assert) {
  // starts your table, then runs your assertions, then purges your table
  assert.end();
});

// Clean up the table when you're done with it
dynamodb.delete();

```

## API

**var dynamodb = require('dynamodb-test')(tape, projectName, tableDef, [region], [port])**

Configure the `dynamodb` object by providing your own `tape` object, an arbitrary name for your project, and the JSON object that defines the table's schema. Optionally, you may specify a region. If you do, tests will be run against a live DynamoDB table in that region. If you don't, tests will be run against a local instance of [dynalite](https://github.com/mhart/dynalite). If you specify a port, dynalite will listen on it, otherwise it will use 4567.

**dynamodb.dynamo**

An instance of [AWS.DynamoDB](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html) configured to query your test endpoint (live or dynalite).

**dynamodb.dyno**

A [dyno](https://github.com/mapbox/dyno) instance configured to work against your test endpoint.

**dynamodb.tableName**

A randomly generated name of the table created for your tests.

**dynamodb.tableDef**

The table definition you provided, with `TableName` modified to match `dynamodb.tableName`

**dynamodb.start()**

Creates the table

**dynamodb.delete()**

Deletes the table.

**dynamodb.load(fixtures)**

By providing an array of [dyno](https://github.com/mapbox/dyno)-style features, dynamodb-test can load them into your test database.

**dynamodb.empty()**

Empties all records from your table, for consistency across tests.

**dynamodb.test(testName, [fixtures], callback)**

A wrapper around [tape](https://github.com/substack/tape) that:

- creates your table or empties it if it already exists
- optionally, loads fixtures that you provide
- runs your tests by providing an `assertion` object to your `callback` function
- empties your table

**dynamodb.close()**

If you're working in a mock test environment, use this call to shut down dynalite.
