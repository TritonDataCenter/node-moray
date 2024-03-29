# Changelog

## v4.1.4

* TOOLS-2525 Everything needs to stop cloning with git:// URLs (again)
  node-fast dependency uses git:// URL

## v4.1.3

* Switch away from git:// URLs

## v4.1.2

* Bump to node-fast@3.1.2 (bump to microtime@3.0.0)

## v4.1.1

* [MANTA-5337](https://jira.joyent.us/browse/MANTA-5337) Fix a bug introduced
  in v4.0.1 that could result in a node-moray install missing required deps.

## v4.1.0

* [TRITON-2142](https://jira.joyent.us/browse/TRITON-2142) add listen() and
  notify() APIs to allow use of PostgreSQL listen/notify features.

## v4.0.2

* Add "files" to package.json to avoid unnecessary voluminous junk in the
  publish npm package.

## v4.0.1

* [MANTA-5186](https://jira.joyent.us/browse/MANTA-5186) switch UUID generation lib from `libuuid` to `uuid`

## v4.0.0

* [MANTA-5170](https://jira.joyent.us/browse/MANTA-5170) moray client metrics should allow additional labels

**Breaking changes:**

* This is a breaking change because it updates node-fast to 3.x. You can read
  about the consequences of that here: https://github.com/joyent/node-fast/blob/master/CHANGES.md#v300

## v3.7.0

* [MANTA-4287](https://jira.joyent.us/browse/MANTA-4287) Update node-moray to use node-fast 2.8.1

## v3.6.0

* [MORAY-494](http://smartos.org/bugview/MORAY-494) node-moray should update fast and artedi dependencies for node-artedi#17

## v3.5.0

* [TRITON-375](http://smartos.org/bugview/TRITON-375) node-moray should expose similar metrics to moray server

## v3.4.2

* [MORAY-455](http://smartos.org/bugview/MORAY-455) moray asks fast to hold onto too many recent RPC requests

## v3.4.1

* [MORAY-422](http://smartos.org/bugview/MORAY-422) node-moray could smooth over connection shuffling

## v.3.4.0

* [MORAY-428](https://smartos.org/bugview/MORAY-428) Make it safer to use reindexing buckets

## v.3.3.0

* [MANTA-3371](https://smartos.org/bugview/MANTA-3371) triggers cannot be installed with any vnodes in read-only mode

## v3.2.1

* [MORAY-409](http://smartos.org/bugview/MORAY-409) Moray client needs to pass timeout to server for sql()

## v3.2.0

* [MORAY-104](https://smartos.org/bugview/MORAY-104) disallow filtering on non-indexed fields except interactively

## v3.1.1

* [MORAY-396](https://smartos.org/bugview/MORAY-396) update to cueball v2

## v3.1.0

* [MORAY-254](https://smartos.org/bugview/MORAY-254) moray tools need manual
  pages

## v3.0.0

* [MORAY-280](https://smartos.org/bugview/MORAY-280) Support SRV records in node-moray
* [MORAY-381](https://smartos.org/bugview/MORAY-381) want default values with cueballOptions
* [MORAY-380](https://smartos.org/bugview/MORAY-380) translateLegacyOptions not setting "service"
* [MORAY-383](https://smartos.org/bugview/MORAY-383) moray client log entries could include specific backend details
* [MORAY-384](https://smartos.org/bugview/MORAY-384) moray's default connection backoff need to be much less aggressive
* [MORAY-385](https://smartos.org/bugview/MORAY-385) moray tools need some cleanup
* [MORAY-349](https://smartos.org/bugview/MORAY-349) remove "backfill" command

**Breaking changes:**

* Constructor arguments have changed.  The only change that will explicitly
  break existing v2 consumers is that the `cueballOptions.domain` option is no
  longer supported.  However, it's strongly recommended that all consumers
  update their constructor arguments to support SRV-based service discovery with
  bootstrap resolvers.  Best practices (with examples) are included in the
  `moray(3)` manual page.  See [RFD
  73](https://github.com/joyent/rfd/tree/master/rfd/0073) for details on the
  reasons for the change.
* The `backfill` command has been removed.  Use `reindexobjects` instead.


## v2.0.1

* [MORAY-377](http://smartos.org/bugview/MORAY-377) moray client masks callers'
  failing to add "error" listeners

## v2.0.0

This is a major rewrite of the guts of this module, primarily to improve
scalability with large numbers of servers and robustness in the face of
networking failures.

**Breaking changes**:

* The `version()` method has been removed.  See [RFD
  33](https://github.com/joyent/rfd/blob/master/rfd/0033/README.md#compatibility-and-upgrade-impact)
  for details.  This was generally not used correctly.
* Errors emitted by RPC calls have different names, because
  server-side errors are now wrapped with errors that reflect the RPC context.
  For example, instead of SomeServerSideError, you'll have a FastRequestError
  that wraps a FastServerError that wraps a SomeServerSideError.  You can
  restore the old behavior by supplying the `unwrapErrors` constructor
  argument, which will cause the client to emit the same errors it emitted
  before (without the Fast-level wrappers).  **Callers should generally be
  updated to use
  [VError.findCauseByName](https://github.com/joyent/node-verror#verrorfindcausebynameerr-name)
  instead of checking the `name` property directly.**
* `retry.retries` must now be a finite number. Once the number of retries has
  been exceeded for a backend, the "cueball" module will consider that backend
  dead, and will take care of periodically checking it to see if it's returned.
  The recommended and default value is `5`.

**Other changes:**

* The constructor now accepts a more precise set of arguments related to
  timeouts, delays, and retry policies.  See the constructor's comments for
  usage information.  The constructor is backwards-compatible, so you can still
  supply the same arguments that were used by the old client version (though you
  cannot mix old and new styles).
* This version replaced much of the implementation to use
  [node-cueball](https://github.com/joyent/node-cueball) for service discovery
  and connection health management.  Previously, this module did its own
  service discovery that did not support eDNS or TCP-based DNS, so it did not
  scale well with large numbers of servers.  Previously, the node-fast module
  was responsible for connection health management.  However, node-fast didn't
  actually handle explicit network errors; it simply attempted to avoid reading
  and writing on sockets that were no longer readable or writable.  This worked
  surprisingly well in the face of servers simply restarting, but it failed
  badly when the socket experienced a true network failure (like ETIMEDOUT or
  ECONNRESET).  Since both of these problems are both hard and need to be solved
  for other components (e.g., HTTP-based components), they were separated into
  the new "cueball" module.
* Related to that, this version replaced the Fast protocol implementation with
  [node-fast version 2](https://github.com/joyent/node-fast).  node-fast v2 is
  substantially simpler than the previous implementation of fast (because it
  does not deal with service discovery or connection health management) and much
  more robust to protocol errors than the previous implementation was.

Specific issues fixed:

* [MORAY-362](http://smartos.org/bugview/MORAY-362) reindexobjects always fails on missing vasync dependency
* [MORAY-361](http://smartos.org/bugview/MORAY-361) moray client tools hang forever while moray is down
* [MORAY-346](http://smartos.org/bugview/MORAY-346) moray client needs work
* [MORAY-309](http://smartos.org/bugview/MORAY-309) error events should be emitted with prudence
* [MORAY-257](http://smartos.org/bugview/MORAY-257) MorayClient should emit errors properly
* [MORAY-300](http://smartos.org/bugview/MORAY-300) node-moray requires log parameter unnecessarily for version and ping
* [MORAY-334](http://smartos.org/bugview/MORAY-334) minnow clients reporting no active connections when moray seems to be up
* [MORAY-356](http://smartos.org/bugview/MORAY-356) moray client continuing to periodically check DNS after close
* [MORAY-325](http://smartos.org/bugview/MORAY-325) node-moray client emits 'close' event even if some connections are still open
* [MORAY-365](http://smartos.org/bugview/MORAY-365) command-line tools are not checked for style or lint
* [MORAY-366](http://smartos.org/bugview/MORAY-366) want command-line tool for "gettokens" RPC call
* [MORAY-238](http://smartos.org/bugview/MORAY-238) node-moray retry policy does not match what's configured
* [MORAY-357](http://smartos.org/bugview/MORAY-357) moray errors indict client for server-side problem


## v1 and earlier

Major version 1 and earlier (including v1.0.1 and everything before that) were
distributed using git URLs, not npm.  As a result, they didn't really support
semver.  These may be retroactively published to npm to aid the transition to
v2.
