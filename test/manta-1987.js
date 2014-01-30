var assert = require('assert-plus');
var bunyan = require('bunyan');
var moray = require('moray');

var client = moray.createClient({
    dns: {
        resolvers: ['10.77.77.6']
    },
    host: '1.moray.coal.joyent.us',
    port: 2020,
    log: bunyan.createLogger({
        name: 'moray',
        level: process.env.LOG_LEVEL || 'trace',
        stream: process.stdout,
        serializers: bunyan.stdSerializers
    }),
    src: true
});

client.on('error', function (err) {
    console.error(err.stack);
    process.exit(1);
});

client.on('connect', function () {
    console.log('starting...');
    (function run() {
        var res = client.find('manta', '_id=1091');
        res.once('error', function (err) {
            console.error(err.stack);
            if (err.name !== 'NoConnectionError' &&
                err.name !== 'ConnectionClosedError') {
                process.exit(1);
            } else {
                run();
            }
        });
        res.once('end', run);
    })();
});
