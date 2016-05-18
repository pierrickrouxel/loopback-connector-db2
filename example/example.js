// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-db2
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

var DataSource = require('loopback-datasource-juggler').DataSource;
var DB2i = require('../'); // loopback-connector-db2i

var config = {
  useLimitOffset: true,
  schema: process.env.DB2_SCHEMA || 'STRONGLOOP',
  naming: process.env.DB2_NAMING,
  username: process.env.DB2_USERNAME,
  password: process.env.DB2_PASSWORD
};

var db = new DataSource(DB2i, config);

var User = db.define('User', {name: {type: String}, email: {type: String} });

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

  User.find({where: {name: 'Tony'}}, function(err, users) {
    console.log(err, users);
  });

  User.destroyAll(function() {
    console.log('example complete');
  });
});
