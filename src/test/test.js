//Archan patel
//Partition testing script

const _ = require('underscore');
const promisify = require('abacus-yieldable').promise;
const partition = require('abacus-partition');
const dbclient = require('abacus-dbclient');
const viewCreate = require('../index.js');
const dbUrl = 'http://localhost:5984/';
const prefix = 'testing';

const partitioner = partition.partitioner(
    partition.bucket,
    partition.period,
    partition.forward,
    partition.balance,
    true
   );

var db = dbclient(
  partitioner,
  dbclient.dburi(dbUrl ? dbUrl : '', prefix),
  (uri, opt, cb) => {
   dbclient.dbcons(uri, opt, cb);
  }
);
db.allDocs = promisify(db.allDocs);
db.bulkDocs = promisify(db.bulkDocs);
db.put = promisify(db.put);

const desDocs = [
  {
    _id: '_design/example',
    views: {
      by_title: {
        map: function(doc) { emit(doc.title); }.toString()
      }
    }
  }
];

const URI = dbclient.dburi(dbUrl ? dbUrl : '', prefix)
const dbopt = {
  uri: URI,
  cons: function(uri, opt, cb) { dbclient.dbcons(uri, opt, cb); },
  partitioner: partitioner
}

/*db.bulkDocs([
  {title: 'The Heart of Darkness', _id: 'k/abcde' + '/t/' + 1400000000000},
  {title: 'The Old Man and the Sea', _id: 'k/efghi' + '/t/' + 1400000000000},
  {title: 'The Sound and the Fury', _id: 'k/jklmno' + '/t/' + 1400000000000},
  {title: 'The Brother Karamazov', _id: 'k/pqrst' + '/t/' + 1400000000000}
]).then((res) => {
  console.log(res);
}).catch((err) => {
  console.log(err);
});*/

const time = 1410000000000;
const exTime = 1400000000000;
const key = 'abcde';
const sKey = '';
const opt = {
  include_docs: true,
  endkey: 'k/' + 'a' + '/t/' + time,
  startkey: 'k/' + 'a' + '/t/' + exTime,
}

viewCreate.addDesignDoc(dbopt, desDocs, opt, (err, res) => {
  console.log('CALLED CB' + JSON.stringify(res));
});

viewCreate.queryDesignDoc(
  dbopt, 'example/by_title', opt, (err, res) => {
  console.log('QUERY: ' + JSON.stringify(res));
});


viewCreate.removeDesignDoc(
  dbopt, '_design/example', opt, (err, res) => {
    console.log("REMOVE: " + JSON.stringify(res));
  });
