# moray 1 "January 2017" Moray "Moray Client Tools"

## NAME

moray - command-line tools for Moray

## DESCRIPTION

Moray is a JSON-based key-value store.  Users can create **buckets**, each
containing any number of **objects** indexed by a primary **key**.  Additional
indexes can be specified with each bucket.  Moray servers are typically
stateless components deployed atop PostgreSQL databases, where buckets
correspond to database tables, objects correspond to rows, and database indexes
are created for each bucket index.

The `moray` npm package contains a set of command-line tools to allow users to
create, update, and delete both buckets and objects.  These tools connect to one
or more remote Moray servers over TCP and execute operations.

Working with buckets:

* `putbucket`: create or update a bucket
* `getbucket`: print detailed information about one bucket
* `listbuckets`: print detailed information about all buckets
* `delbucket`: delete a bucket and all of its contents

Working with objects:

* `putobject`: create or update an object
* `getobject`: fetch the contents of an object by primary key
* `delobject`: delete an object by primary key
* `findobjects`: fetch the contents of multiple objects using a filter
* `delmany`: delete multiple objects using a filter
* `updatemany`: update multiple objects using a filter
* `reindexobjects`: populate a newly-added index

Working with remote servers:

* `morayping`: check whether Moray is online
* `morayversion`: check the version of a Moray server
* `sql`: execute a SQL string on a Moray server
* `gettokens`: fetch the list of shards from electric-moray

The tools in this package support two modes for locating the remote Moray server
on which to operate:

* Using the `-S`/`--service SERVICE_NAME` option or the `MORAY_SERVICE`
  environment variable, users specify a DNS domain to which SRV records are
  attached that describe the list of instances available.  SRV records provide
  both a name for the host (which may be an IP address or another DNS domain)
  and a port on which to connect over TCP.  This mode is preferred for
  general use because it provides information about all instances and allows the
  client to balance multiple requests across different, equivalent servers.
* Using the `-h`/`--host HOST_OR_IP` and `-p`/`--port PORT` options or the
  `MORAY_URL` environment variable, users specify a specific IP address or DNS
  domain to which traditional name records are attached and a TCP port to which
  to connect.  This is useful primarily for testing against specific server
  instances.

If the `-S`/`--service SERVICE_NAME` command-line option is specified, it is
always used directly as described above.

If the `-h`/`--host HOST_OR_IP` or `-p`/`--port PORT` options are specified,
they are used directly as described above.  If one is specified and not the
other, then the other value is filled in from the `MORAY_URL` environment
variable.  Otherwise, defaults of IP `127.0.0.1` and port `2020` are used.

If none of these command-line options are specified:

- if `MORAY_SERVICE` is specified, it is used to invoke the first mode
- if `MORAY_URL` is specified, is used to invoke the second mode
- if neither is specified, the second mode is invoked with default values
  `127.0.0.1` port `2020`.

## OPTIONS

The following `COMMON_OPTIONS` options are accepted by all of these commands:

`-b, --bootstrap-domain BOOTSTRAP_DOMAIN`
  Specifies the domain name for the nameservers themselves.  Triton and Manta
  both provide domain names for the nameservers themselves.  This is useful in
  split DNS environments to ensure that the Moray only uses the nameservers
  that know about the target service.  This applies to both SRV-record-based
  discovery and traditional A-record-based discovery, but has no impact when
  connecting to a specific IP address and port.

`-h, --host HOST_OR_IP`
  Specifies an IP address or DNS domain for the remote Moray server.  See
  above for details.

`-p, --port PORT`
  Specifies the TCP port for the remote Moray server.  See above for details.

`-S, --service SERVICE`
  Specifies a DNS domain to be used for SRV-based service discovery of the
  remote Moray server.  See above for details.  `SERVICE` must not be an IP
  address.

`-v, --verbose`
  Increases the verbosity of the built-in bunyan logger.  By default, the
  logger is created with bunyan level `fatal`.  Each additional use of `-v`
  increases the verbosity by one level (to `error`, `warn`, and so on).  Log
  messages are emitted to stderr.  See also the `LOG_LEVEL` environment
  variable.

## ENVIRONMENT

`LOG_LEVEL`
  Sets the node-bunyan logging level. Defaults to "fatal".

`MORAY_BOOTSTRAP_DOMAIN`
  Used as a fallback value for the `-b`/`--bootstrap-domain` option.

`MORAY_SERVICE`
  Used as a fallback value for `-S`/`--service` if neither of `-h`/`--host` or
  `-p`/`--port` is specified.

`MORAY_URL`
  A URL of the form `tcp://HOSTNAME_OR_IP[:PORT]` where the specified
  `HOSTNAME_OR_IP` and `PORT` will be used as fallback values for the
  `-h`/`--host` or `-p/--port` options, respectively.  This value is only used
  if `MORAY_SERVICE` is not present in the environment and at least one of the
  `-h`/`--host` or `-p`/`--port` options is not specified.

## EXIT STATUS

0
  Indicates successful completion

1
  Indicates failure

2
  Indicates an invalid invocation (usage error)


## EXAMPLES

Create a bucket for "accounts" version 1 with unique "loginName" and unique
numeric values for "uid":

    $ putbucket -x 1 -u loginName -u uid:number accounts

Insert a few accounts.  The keys will match the login names:

    $ putobject -d '{ "loginName": "lisa", "uid": "800", "country": "USA" }' \
        accounts lisa
    $ putobject -d '{ "loginName": "hugh", "uid": "801", "country": "UK" }' \
        accounts hugh

Fetch one of these back:

    $ getobject accounts lisa
    {
      "bucket": "accounts",
      "key": "lisa",
      "value": {
        "loginName": "lisa",
        "uid": 800,
        "country": "USA"
      },
      "_id": 1,
      "_etag": "77472568",
      "_mtime": 1485539314987,
      "_txn_snap": null,
      "_count": null
    }

Fetch objects having uids at most 800:

    $ findobjects accounts '(uid<=800)'
    {
      "bucket": "accounts",
      "key": "lisa",
      "value": {
        "loginName": "lisa",
        "uid": 800
      },
      "_id": 1,
      "_etag": "77472568",
      "_mtime": 1485539314987,
      "_txn_snap": null,
      "_count": 1
    }

Update the bucket to version 2, which contains a new non-unique indexed column
for "country":

    $ putbucket -x 2 -u loginName -u uid:number -i country accounts

Re-index the objects in the bucket:

    $ reindexobjects accounts
    bucket "accounts": 2 objects processed (continuing)
    bucket "accounts": all objects processed

Now we can search for accounts by country:

    $ findobjects accounts '(country=UK)'
    {
      "bucket": "accounts",
      "key": "hugh",
      "value": {
        "loginName": "hugh",
        "uid": 801,
        "country": "UK"
      },
      "_id": 2,
      "_etag": "82E66E74",
      "_mtime": 1485539319781,
      "_txn_snap": null,
      "_count": 1
    }

Update "country" for accounts with country = "UK":

    $ updatemany -d '{ "country": "United Kingdom" }' accounts '(country=UK)'
    { count: 1, etag: 'ue6d321d' }

Now fetch back "hugh":

    $ getobject accounts hugh
    {
      "bucket": "accounts",
      "key": "hugh",
      "value": {
        "loginName": "hugh",
        "uid": 801,
        "country": "United Kingdom"
      },
      "_id": 2,
      "_etag": "ue6d321d",
      "_mtime": 1485539410157,
      "_txn_snap": null,
      "_count": null
    }

Now delete that object:

    $ delobject accounts hugh

List everything in the bucket (more precisely, everything having a non-null
"loginName"):

    $ findobjects accounts '(loginName=*)'
    {
      "bucket": "accounts",
      "key": "lisa",
      "value": {
        "loginName": "lisa",
        "uid": 800,
        "country": "USA"
      },
      "_id": 1,
      "_etag": "77472568",
      "_mtime": 1485539314987,
      "_txn_snap": null,
      "_count": 1
    }

Finally, remove the bucket:

    $ delbucket accounts

## SEE ALSO

`moray(3)`, `delbucket(1)`, `delmany(1)`, `delobject(1)`, `findobjects(1)`,
`getbucket(1)`, `getobject(1)`, `gettokens(1)`, `listbuckets(1)`,
`morayping(1)`, `morayversion(1)`, `putbucket(1)`, `putobject(1)`,
`reindexobjects(1)`, `sql(1)`, `updatemany(1)`,
[jsontool](https://github.com/trentm/json)

## DIAGNOSTICS

See the `-v`/`--verbose` option and the `LOG_LEVEL` environment variable.
