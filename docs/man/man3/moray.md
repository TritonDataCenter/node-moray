# moray 3 "January 2017" Moray "Moray Client Library"

## NAME

moray - Node.js client library for Moray

## DESCRIPTION

Moray is a JSON-based key-value store.  For an overview of Moray, see
`moray(1)`.  This manual page assumes basic familiarity with the services Moray
provides, how to use them from the command line, and how both SRV-based service
discovery and traditional service discovery work from the command line.

The `moray` npm package contains a Node.js client library that allows consumers
to create, update, and delete both buckets and objects.  The package also
contains a suite of command-line tools for exercising these interfaces.  When in
doubt, see the implementation of those tools for examples of using each RPC
call.

### Creating a client

To interact with Moray, users of the Node library instantiate a Moray client
object, which requires a bunyan-style logger.  The simplest invocation, which is
suitable only for one-off tools and test programs, would be to connect to Moray
on localhost:

    var mod_bunyan = require('bunyan');
    var mod_moray = require('moray');

    var log = mod_bunyan.createLogger({ name: 'my-tool', level: 'INFO' });

    var client = mod_moray.createClient({
        log: log,
        host: '127.0.0.1',
        port: 2020
    });

    client.on('connect', function () {
        /* Start making RPC calls.  This example lists buckets. */
        client.listBuckets(function (err, buckets) {
            if (err) {
                /* This should be handled more gracefully! */
                throw (err);
            }

            console.log(JSON.stringify(buckets));

            /* When finished, be sure to close the client. */
            client.close();
        });
    });

This works for a simple test program, but is not suitable for Triton and Manta
servers or command-line tools for a few reasons:

* This mode does not support SRV-based service discovery, which is critical for
  increased scalability and improved fault tolerance.  SRV-based discovery is
  configured by using the `srvDomain` property instead of `host` or `port.`
* This mode does not enable the client to use bootstrap resolvers, which are
  critical for use in mixed DNS environments (e.g., where a combination of
  Triton, Manta, or external namservers may be in use).  Bootstrap resolvers are
  configured using the `cueballOptions.resolvers` property.
* Command-line tools should generally specify additional parameters to ensure
  that they fail quickly when servers are down rather than retrying
  indefinitely until they are online.  This means specifying `failFast`.  It's
  also a good idea to specify `mustCloseBeforeNormalProcessExit` to make sure
  that your tool shuts down cleanly.

Here's a general pattern for *server* components in Triton and Manta to
configure the Moray client:

    var mod_jsprim = require('jsprim');

    var client, config;

    /*
     * Extract the Moray client configuration block from the server's
     * configuration file.  Here, we assume that "serverConfig" came from
     * parsing the server's configuration file.  We also assume that the
     * Moray configuration property is just called "moray", but some components
     * (notably Muskie) have multiple clients, and they would use different
     * property names (e.g., "picker" or "marlin").
     */
    config = jsprim.deepCopy(serverConfig.moray);
    config.log = log;
    client = mod_moray.createClient(config);
    client.on('connect', function onMorayConnect() {
        /* start using the client */
    });

*Client* components would usually add an `error` listener, too:

    /*
     * Client tools add an error listener.  Servers generally should NOT do this
     * because they configure the client to retry indefinitely.  Any errors
     * emitted by the client would be programmer errors.
     */
    client.on('error', function onError(err) {
        console.error('moray client error: %s', err.message);
        process.exit(1);
    });

In practice, `serverConfig.moray` comes from a SAPI configuration template.  For
**Triton services**, it will typically look like this:

    {
        "srvDomain": "{{{MORAY_SERVICE}}}"
        "cueballOptions": {
            "resolvers": [ "{{{BINDER_SERVICE}}}" ]
        }
    }

That will expand to something like this:

    {
        "srvDomain": "moray.mydatacenter.joyent.us",
        "cueballOptions": {
            "resolvers": [ "binder.mydatacenter.joyent.us" ]
        }
    }

For **Manta services**, the template file will typically include a block that
looks like this:

    {
        "srvDomain": "{{MARLIN_MORAY_SHARD}}",
        "cueballOptions": {
            "resolvers": [ "nameservice.{{DOMAIN_NAME}}" ]
        }
    }

That will expand to something like this:

    {
        "srvDomain": "1.moray.myregion.joyent.us",
        "cueballOptions": {
            "resolvers": [ "nameservice.myregion.joyent.us" ]
        }
    }

This approach (using a block from the configuration file) allows operators to
reconfigure a service to point at a specific instance by replacing the
`srvDomain` property with `host` and `port` properties.

Command-line tools that use Moray should typically define their own options for
specifying `srvDomain`, `host`, and `port` properties.  See `moray(1)` for the
command-line options and fallback environment variables used by the built-in
Moray tools.

Command-line tools should generally also specify `failFast` and
`mustCloseBeforeNormalProcessExit`.


### Making RPC calls

Callers make RPC calls by invoking RPC methods on the client.  The specific
methods are documented in the [Moray server](https://github.com/joyent/moray)
reference documentation.

All RPC methods are asynchronous, and they all follow one of two styles
described in the "Node.js Error Handling" documentat.  The style used depends on
the kind of data returned by the RPC.

* RPC calls that return a fixed number of results (usually just one object or a
  small chunk of metadata) are callback-based: the last argument to the RPC
  method is a callback.  The first argument to the callback is an optional
  error, and subsequent arguments are RPC-specific.
* RPC calls that return a large or variable number of results (like
  `findObjects`) are event-emitter-based: they return an event emitter that
  emits `error` on failure, `end` on completion, and other events depending on
  the RPC call.

All of the RPC methods take an optional `options` argument that is always the
last non-callback argument.  (For callback-based RPCs, it's the second-to-last
argument.  For event-emitter-based RPCs, it's the last argument.)  You can use
this to pass in a `req_id` for correlating log entries from one service with
the Moray client log entries.  Some APIs (namely put/get/del object) have
additional options to allow cache bypassing, for example.


## OPTIONS

The client constructor uses named arguments on a single `args` object.

All constructor invocations must provide:

`log` (object)
  a bunyan-style logger

All constructor invocations must also provide one of the following:

`srvDomain` (string)
  DNS domain name for SRV-based service discovery

`url` (string)
  Describes the hostname or IP address and TCP port to specify a specific
  Moray server to connect to (instead of using SRV-based service discovery).
  This is deprecated for servers, and should only be used for tools, for
  testing, and for unusual, temporary operational changes.  The format for
  this option is the same as for the `MORAY_URL` environment variable
  described in `moray(1)`.

`host` (string) and `port` (integer or string)
  Like `URL`, but specified using different properties.

Callers may also provide:

`cueballOptions` (object)
  Overrides cueball-related options, including various timeouts and delays.
  For specific options that can be overridden here, see the source.  **NOTE:
  it's not expected that most consumers would need to specify any of these.
  Default values ought to work for the port, DNS service, and all the various
  timeouts, delays, and retry limits.**

`failFast` (boolean)
  If true, this sets a more aggressive retry policy, and the client emits
  "error" when the underlying Cueball set reaches state "failed".  This is
  intended for use by command-line tools to abort when it looks like dependent
  servers are down.  Servers should generally not specify this option because
  they should wait indefinitely for dependent services to come up.

`unwrapErrors` (boolean)
  If false (the default), Errors emitted by this client and RPC requests will
  contain a cause chain that explains precisely what happened.  For example,
  if an RPC fails with SomeError, you'll get back a FastRequestError
  (indicating a request failure) caused by a FastServerError (indicating that
  the failure was on the remote server, as opposed to a local or
  transport-level failure) caused by a SomeError.  In this mode, you should
  use VError.findCauseByName(err, 'SomeError') to determine whether the root
  cause was a SomeError.
  If the "unwrapErrors" option is true, then Fast-level errors are unwrapped
  and the first non-Fast error in the cause chain is returned.  This is
  provided primarily for compatibility with legacy code that uses err.name to
  determine what kind of Error was returned.  New code should prefer
  VError.findCauseByName() instead.

`mustCloseBeforeNormalProcessExit` (boolean)
  If true, then cause the program to crash if it would otherwise exit 0 and
  this client has not been closed.  This is useful for making sure that client
  consumers clean up after themselves.

`requireIndexes` (boolean)
  If true, all `findObjects` requests sent from this client will respond with a
  `NotIndexedError` error if at least one of the fields included in the search
  filter has an index that can't be used.

  If the server that handles a given `findObjects` request does not support
  checking that search fields have usable indexes, an `UnhandledOptionsError`
  event will be emitted. In this case, the error object will have a property
  named `unhandledOptions` whose value is an array of strings that will contain
  the string `'requireIndexes'`, to represent that this option wasn't handled by
  the moray server that served the `findObjects` request.

  Passing `requireIndexes: false` to any `findObjects` request will disable this
  behavior for that specific request, regardless of the value of the
  `requireIndexes` option passed when instantiating the client.

Some legacy options are accepted as documented in the source.

## ENVIRONMENT

The `LOG_LEVEL`, `MORAY_SERVICE`, and `MORAY_URL` environment variables are
interpreted by each command-line tool, not the client library itself.


## SEE ALSO

`moray(1)`, [Moray server reference
documentation](https://github.com/joyent/moray/blob/master/docs/index.md),
[Node.js Error
Handling](https://www.joyent.com/node-js/production/design/errors).


## DIAGNOSTICS

The client library logs messages using the bunyan logger.  Increase the log
level for more verbose output.

The underlying [node-fast](https://github.com/joyent/node-fast) RPC library
provides DTrace probes on supported systems for inspecting low-level RPC events.

Use Node's `--abort-on-uncaught-exception` command-line argument to enable core
file generation upon fatal program failure.  These core files can be used with
[mdb_v8](https://github.com/joyent/mdb_v8) to inspect the program's state at the
time of the crash.
