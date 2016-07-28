'use strict'

/*
  This is a script that can add and remove docs from multple abacus database.
  This allows the user to place a design document in a database(s) for faster
  querying and more efficient debugging.
*/

const _ = require('underscore');
const partition = require('abacus-partition');
const tranform = require('abacus-tranform');
const perf = require('abacus-perf');
const PouchDB = require('pouchdb');
const debug = require('abacus-debug')('abacus-viewcreate');

const flatten = _.flatten;
const map = _.map;
const first = _.first;
const sortBy = _.sortBy;
const extend = _.extend;

// Post-process db errors and mark them such that they nicely flow through
// circuit breakers and retries
const error = (err) => {
  if(!err)
    return err;
  if(err.status !== 409 && err.status !== 404)
    return err;
  // Warning: mutating variable err, but that's intentional
  err.noretry = true;
  err.nobreaker = true;
  return err;
};

// Adds the design doc to each database specified by the options.
const designDocCouch = function(couchclient, dbopt, op, docs, opt) {
  // Get the key to determine the bucket of the database.
  const k = key(docs[0]._id) ? key(docs[0]._id) === key(docs[1]._id) ?
    key(docs[0]._id) : undefined : undefined;

  const opfunction = function(err, dbs) => {
    if(err) {
      lcb(err);
      return;
    }
    // Apply the given db operation to each db and accumulate the results
    transform.reduce(dbs, (accum, db, i, dbs, rcb) => {
      // Stop once we've accumulated the requested number of rows
      if(opt.limit && accum.length === opt.limit) {
        rcb(undefined, accum);
        return;
      }
      // If db is an array, search in all dbs.
      if(Array.isArray(db))
        transform.map(db, (v, i, l, mcb) => {
          debug('Running operation in db %s', v._db_name);
          op(v, docs, opt.limit ? extend({}, opt, {
            limit: opt.limit - accum.length + skip,
            skip: 0
          }) : extend({}, opt, { skip: 0 }),
          (err, rows) => err ? mcb(err) : mcb(undefined, rows));
        }, (err, rows) => {
          if(err) {
            rcb(err);
            return;
          }
          // Flatten the rows from dbs and sort them.
          const sr = opt.descending ? sortBy(flatten(rows, true),
          (r) => r.id).reverse() : sortBy(flatten(rows, true), (r) => r.id);
          debug(sr);
          rcb(undefined, opt.limit ? accum.concat(first(sr, opt.limit
            - accum.length + skip)) : accum.concat(sr));
        });
      else
        op(db, docs, opt.limit ? extend({}, opt, {
          limit: opt.limit - accum.length + skip
        }) : opt,
          (err, rows) => err ? rcb(err) : rcb(undefined, accum.concat(rows)));
    }, [], lcb);
  }

  const partitions = [];
  const pool = function(dbopt, p, rw, cb) {
    if(Array.isArray(pars[0]))
      return transform.map(p, (v, i, p, mcb) => {
        const u = dbopt.uri(v);
        debug('Using db %s in %s mode', puri(u), rw);

        // Return memoized db partition handle or get and memoize a new one
        // from the given db constructor. DB handles are keyed by db uri and
        // read/write operating mode
        const dbk = [u, rw].join('-');
        if (partitions[dbk])
          return mcb(null, partitions[dbk]);

        debug('Constructing db handle for db %s in %s mode', puri(u), rw);
        return dbopt.cons(u, {
          // Skip db setup in read mode, as we don't need the db to be
          // created if it doesn't exist
          skip_setup: rw === 'read'
        }, (err, db) => {
          if(err) {
            mcb(null, errdb('dbcons-err-' + u, err));
            return;
          }

          // Warning: mutating variable partitions
          // Memoize the db handle with both the read mode and the
          // requested read/write mode
          partitions[[u, 'read'].join('-')] = db;
          partitions[dbk] = db;

          mcb(null, db);
        });
      }, (err, res) => {
        cb(err, res);
      });
    const u = dbopt.uri(p);
    debug('Using db %s in %s mode', puri(u), rw);

    // Return memoized db partition handle or get and memoize a new one
    // from the given db constructor. DB handles are keyed by db uri and
    // read/write operating mode
    const dbk = [u, rw].join('-');
    if (partitions[dbk])
      return cb(null, partitions[dbk]);

    debug('Constructing db handle for db %s in %s mode', puri(u), rw);
    return dbopt.cons(u, {
      // Skip db setup in read mode, as we don't need the db to be
      // created if it doesn't exist
      skip_setup: rw === 'read'
    }, (err, db) => {
      if(err) {
        cb(null, errdb('dbcons-err-' + u, err));
        return;
      }

      // Warning: mutating variable partitions
      // Memoize the db handle with both the read mode and the
      // requested read/write mode
      partitions[[u, 'read'].join('-')] = db;
      partitions[dbk] = db;

      cb(null, db);
    });
  }

  // Find the date-bucket pairs for the databases [YYYYMM, bucket]
  const time = couchclient.time;
  partitioner(k, [ time(opt.startKey), time(opt.endKey) ], 'write',
    (err, pars) => {
      err ? cb(err): transform.map(pars, (p, i, pars, pcb) =>
        pool(p, 'write', pcb), opfunction)
    });
}

const addDesignDoc = function(couchclient, dbopt, docs, opt, cb) {
  const op = function(db, docs, opt, cb) {
    db.bulkDocs(docs, opt,
      (err, res) => err ? cb(error(err)) : cb(null, res));
  }

  designDocCouch(couchclient, dbopt, op, docs, opt);
}

const removeDesignDoc = function(couchclient, dbopt, docs, opt, cb) {
  const op = function(db, docs, opt, cb) {
    db.bulkDocs(docs, opt,
      (err, res) => err ? cb(error(err)) : cb(null, res));
  }

  designDocCouch(couchclient, dbopt, op, docs, opt);
}
