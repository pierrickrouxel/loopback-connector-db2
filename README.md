# Project replacement
This development is discontinued. Instead, please use the `loopback-connector-ibmi` connector. You can find the new connector [here](http://github.com/StrongLoop/loopback-connector-ibmi). It is created by [the StrongLoop and IBM organization](http://github.com/StrongLoop/).

## TODO
* Parse date
* Use the [loopback-ibmdb](https://github.com/strongloop/loopback-ibmdb) module
* Fix `naming` configuration (actually always system)
* Remove patch for prepare statement when IBM will fix the driver

# loopback-connector-db2i

The `loopback-connector-db2i` module is the LoopBack connector for DB2 for IBM i.

The LoopBack DB2 connector supports:

- All [CRUD operations](https://docs.strongloop.com/display/LB/Creating%2C+updating%2C+and+deleting+data).
- [Queries](https://docs.strongloop.com/display/LB/Querying+data) with fields, limit, order, skip and where filters.
- Only native connection is supported (for the moment). Column organized tables are not supported.

## Installation

Enter the following in the top-level directory of your LoopBack application:

```
$ npm install loopback-connector-db2i --save
```

The `--save` option adds the dependency to the application's `package.json` file.

## Configuration

Use the [data source generator](https://docs.strongloop.com/display/LB/Data+source+generator) (`slc loopback:datasource`) to add the DB2 data source to your application.
The entry in the application's `server/datasources.json` will look something like this:

```
"mydb": {
  "name": "mydb",
  "connector": "db2i"
}
```

Edit `server/datasources.json` to add other supported properties as required:

```
"mydb": {
  "name": "mydb",
  "connector": "db2i",
  "username": <username>,
  "password": <password>,
  "database": <database name>,
  "hostname": <db2 server hostname>,
  "port":     <port number>
}
```

The following table describes the connector properties.

Property       | Type    | Description
---------------| --------| --------
database       | String  | Database name
schema         | String  | Specifies the default schema name that is used to qualify unqualified database objects in dynamically prepared SQL statements. The value of this property sets the value in the CURRENT SCHEMA special register on the database server. The schema name is case-sensitive, and must be specified in uppercase characters
username       | String  | DB2 Username
password       | String  | DB2 password associated with the username above
hostname       | String  | DB2 server hostname or IP address
port           | String  | DB2 server TCP port number
useLimitOffset | Boolean | LIMIT and OFFSET must be configured on the DB2 server before use (compatibility mode)

Alternatively, you can create and configure the data source in JavaScript code.
For example:

```
var DataSource = require('loopback-datasource-juggler').DataSource;
var DB2 = require('loopback-connector-db2');

var config = {
  username: process.env.DB2_USERNAME,
  password: process.env.DB2_PASSWORD,
  hostname: process.env.DB2_HOSTNAME,
  port: 50000,
  database: 'SQLDB',
};

var db = new DataSource(DB2, config);

var User = db.define('User', {
  name: { type: String },
  email: { type: String },
});

db.autoupdate('User', function(err) {
  if (err) {
    console.log(err);
    return;
  }

  User.create({
    name: 'Tony',
    email: 'tony@t.com',
  }, function(err, user) {
    console.log(err, user);
  });

  User.find({ where: { name: 'Tony' }}, function(err, users) {
    console.log(err, users);
  });

  User.destroyAll(function() {
    console.log('example complete');
  });
});
```
