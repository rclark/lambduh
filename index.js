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
  var tmpFile = path.join(os.tmpdir(), 'mbtiles', srcName);

  var srcUri = 'mbtiles://' + tmpFile;
  var dstUri = 's3://mapbox/lambda-testing/' + srcName + '/{z}/{x}/{y}';

  s3.getObject({
    Bucket: srcBucket,
    Key: srcKey
  }).createReadStream()
    .on('error', callback)
    .pipe(fs.createWriteStream(tmpFile))
    .on('error', callback)
    .on('finish', function() {
      new MBTiles(srcUri, function(err, src) {
        if (err) return callback(err);
        new S3(dstUri, function(err, dst) {
          src.createZXYStream()
            .pipe(tilelive.createReadStream(src, { type: 'list' }))
            .on('error', callback)
            .pipe(tilelive.createWriteStream(dst))
            .on('error', callback)
            .on('stop', function() {
              console.log('Finished %s', dstUri);
              callback();
            });
        });
      });
    });
};
