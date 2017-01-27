# putbucket 1 "January 2017" Moray "Moray Client Tools"

## NAME

putbucket - create or update a bucket

## SYNOPSIS

`putbucket [COMMON_OPTIONS] [-st] [-i FIELD[:TYPE]...] [-u FIELD[:TYPE]...] [-x VERSION] BUCKET`

## DESCRIPTION

Creates or updates a bucket in Moray called `BUCKET`.  If the bucket does not
exist, it is created.  If the bucket already exists and has a version, the
bucket will only be updated if `-x` is specified and `VERSION` is greater than
the version of the bucket on the server.

A bucket's configuration includes:

* a list of field names (also referred to as a **schema**) that describes the
  top-level properties for which database indexes should be created.  These
  indexes enable querying and manipulation using filters accepted by the
  `findobjects`, `updatemany`, and `delmany` tools.
* an optional version number, which is used to coordinate schema updates among
  multiple clients having different versions.
* a set of options defined by the server.  The only option supported by this
  tool is `trackModification`.
* `pre` and `post` triggers, which are functions executed on the Moray server

Users may specify the bucket configuration using a combination of the `-i`,
`-u`, `-x`, or `-t` options, or using the `-s` option and providing a JSON
description on stdin.  The `-s` option must be used for specifying `pre` and
`post` triggers.

When you use `putbucket(1)` to add a new indexed field, that field will not be
treated as indexed (in filters with `findobjects` and similar tools) until all
objects in the bucket have been reindexed.  See `reindexobjects(1)`.

## OPTIONS

`-i FIELD[:TYPE]`
  Adds an index on the top-level property called `FIELD`.  `FIELD` can
  subsequently be used in filters for `findobjects`, `updatemany`, and
  `delmany` operations.  The optional `TYPE` is interpreted by the server.

`-s`
  Read the bucket's configuration from stdin rather than the other
  command-line options.  Other bucket-related command-line options are
  ignored.

`-t`
  Enable the `trackModification` server-side option for this bucket.

`-u FIELD[:TYPE]`
  Adds an index just like the `[-i]` option, but additionally require that
  values of field `FIELD` must be unique within the bucket.  Attempting to
  insert a second object having the same value of `FIELD` as another object in
  the bucket will fail.  This constraint is maintained by the underlying data
  store.

`-x VERSION`
  Only create or update the bucket if it does not already exist on the server,
  or if it exists with no version number, or if it exists with a version
  number that's older than `VERSION`.  The new `VERSION` is stored with the
  bucket configuration.

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

The JSON read from stdin may have the following properties:

`indexes`
  an object whose properties identify each of the indexed fields.  Each value
  is an object with properties `type` (a string) and `unique` (a boolean).

`options`
  an object describing options interpreted by the server.  This may include
  the `trackModification` option, which should be a boolean value.

`post`, `pre`
  arrays of strings containing JavaScript functions to be executed before
  (`pre`) or after (`post`) various operations.  See server documentation for
  details.  The strings themselves are evaluated in the context of this tool.

`version`
  an integer version number, as would be specified with `-x`

Default values are provided that correspond to this configuration:

    {
        "indexes": {},
        "options": {},
        "pre": [],
        "post": []
    }

By default, the bucket is not versioned.


## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Create a bucket at version 1 called "accounts" with no indexed fields:

    $ putbucket -x 1 accounts

This bucket can be used as a key-value store, but would not support
`findobjects` or other operations that operate on indexed fields.

Create a bucket called "accounts" with a unique "loginName" field and an
indexed "fullName" field:

    $ putbucket -x 1 -u loginName -i fullName accounts

Create the same bucket with version number "3":

    $ putbucket -x 3 -u loginName -i fullName accounts

Attempt to create the same bucket with an older version:

    $ putbucket -x 2 -u loginName -i fullName accounts
    putbucket: moray client ("172.27.10.72:65434" to "172.27.10.40:2022"): request failed: server error: accounts has a newer version than 2 (3)

## SEE ALSO

`moray(1)`, `reindexobjects(1)`

## BUGS

The JSON format is not validated on the client side.  Future versions of this
tool may validate input on the client.
