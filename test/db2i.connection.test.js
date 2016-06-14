// Copyright IBM Corp. 2016. All Rights Reserved.
// Node module: loopback-connector-db2
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

/* eslint-env node, mocha */
process.env.NODE_ENV = 'test';
require('./init.js');
var assert = require('assert');
var DataSource = require('loopback-datasource-juggler').DataSource;

var config;

before(function() {
  config = global.config;
});

describe('testConnection', function() {
  it('should pass with valid settings', function(done) {
    var db = new DataSource(require('../'), config);
    db.ping(function(err) {
      assert(!err, 'Should connect without err.');
      done(err);
    });
  });
});

describe('lazyConnect', function() {
  it('should skip connect phase (lazyConnect = true)', function(done) {
    var dsConfig = {
      host: 'invalid-hostname',
      port: 80,
      database: 'invalid-database',
      username: 'invalid-username',
      password: 'invalid-password',
      lazyConnect: true,
    };
    var ds = getDS(dsConfig);

    var errTimeout = setTimeout(function() {
      done();
    }, 2000);
    ds.on('error', function(err) {
      clearTimeout(errTimeout);
      done(err);
    });
  });

  it('should report connection error (lazyConnect = false)', function(done) {
    var dsConfig = {
      host: 'invalid-hostname',
      port: 80,
      database: 'invalid-database',
      username: 'invalid-username',
      password: 'invalid-password',
      lazyConnect: false,
    };
    var ds = getDS(dsConfig);

    ds.on('error', function(err) {
      err.message.should.containEql('SQLSTATE=');
      done();
    });
  });

  var getDS = function(config) {
    var db = new DataSource(require('../'), config);
    return db;
  };
});
