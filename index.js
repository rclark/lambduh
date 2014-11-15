var MBTiles = require('mbtiles');
var S3 = require('tilelive-s3');
var tilelive = require('tilelive');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var fs = require('fs');
var path = require('path');
var os = require('os');

MBTiles.registerProtocols(tilelive);
S3.registerProtocols(tilelive);

module.exports.copyTiles = function(event, context) {
  console.log(JSON.stringify(event, null, 2));
  console.log('\n------------------------\n');
  console.log(JSON.stringify(context, null, 2));

  function callback(err) {
    if (err) console.error(err);
    context.done();
  }

  var srcBucket = event.Records[0].s3.bucket.name;
	var srcKey = event.Records[0].s3.object.key;
  var srcName = path.basename(srcKey);
  var tmpFile = path.join(os.tmpdir(), srcName);

  var srcUri = 'mbtiles://' + tmpFile;
  var dstUri = 's3://mapbox/lambda-testing/' + srcName + '/{z}/{x}/{y}';

  s3.getObject({
    Bucket: srcBucket,
    Key: srcKey
  }).createReadStream()
    .on('error', function(err) {
      err.message = 'S3READ: ' + err.message;
      callback(err);
    })
    .pipe(fs.createWriteStream(tmpFile))
    .on('error', function(err) {
      err.message = 'FSWRITE: ' + err.message;
      callback(err);
    })
    .on('finish', function() {
      tilelive.load(srcUri, function(err, src) {
        if (err) return callback(err);
        tilelive.load(dstUri, function(err, dst) {
          if (err) return callback(err);
          console.log('--> Start copy of %s to %s', srcUri, dstUri);
          src.createZXYStream()
            .pipe(tilelive.createReadStream(src, { type: 'list' }))
            .on('error', function(err) {
              err.message = 'MBTILESREAD: ' + err.message;
              callback(err);
            })
            .pipe(tilelive.createWriteStream(dst))
            .on('error', function(err) {
              err.message = 'S3WRITE: ' + err.message;
              callback(err);
            })
            .on('stop', function() {
              console.log('Finished %s', dstUri);
              callback();
            });
        });
      });
    });
};
