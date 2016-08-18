var assert = require('assert-plus');
var bunyan = require('bunyan');
var moray = require('../lib');
var vasync = require('vasync');

function connectToMoray(callback) {
    assert.func(callback, 'callback');

    var MORAY_CLIENT_CONFIG = {
        dns: {
            resolvers: ['10.99.99.11']
        },
        host: 'moray.coal.joyent.us',
        port: 2020,
        log: bunyan.createLogger({
            name: 'moray_client',
            level: process.env.LOG_LEVEL || 'fatal',
            stream: process.stdout,
            serializers: bunyan.stdSerializers
        })
    };

    var morayClient = moray.createClient(MORAY_CLIENT_CONFIG);

    morayClient.once('connect', function onConnect() {
        morayClient.removeAllListeners('error');
        return (callback(null, morayClient));
    });

    morayClient.on('error', function onError(err) {
        morayClient.removeAllListeners('connect');
        return (callback(err));
    });
}

function closeMorayConnection(morayClient, callback) {
    assert.object(morayClient, 'morayClient');
    assert.func(callback, 'callback');

    morayClient.on('close', function onMorayClientClosed(err) {
        return (callback(err));
    });

    morayClient.close();
}

vasync.waterfall([
    connectToMoray,
    closeMorayConnection
], function allDone(err) {
    assert.ifError(err);
    assert.equal(process._getActiveHandles(), 0,
        'no handle should be left active when moray client connection ' +
        'is closed');
    });
