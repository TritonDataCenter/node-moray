# getobject 1 "January 2017" Moray "Moray Client Tools"

## NAME

getobject - fetch the contents of an object by primary key

## SYNOPSIS

`getobject [COMMON_OPTIONS] [-Hs] BUCKET KEY`

## DESCRIPTION

Fetches the contents of the object in `BUCKET` having primary key `KEY`.  The
result is emitted as JSON with properties:

`bucket`
  matches BUCKET

`key`
  matches KEY

`value`
  the contents of the object, which are completely user-defined

`_id`
  a unique, integer id associated with each object.  It should not be assumed
  that these ids are assigned in any particular order, and the id may change
  across certain kinds of updates.  Critically, if a caller inserts objects 1
  and 2 concurrently and another caller sees object 2, then it may also see
  object 1, but it may not.  Ids may be assigned out of insertion order.

`_etag`
  a numeric value calculated from the contents of the object.  This can be
  used for conditional put operations.  See `putobject(1)`.

## OPTIONS

`-H`
  Print the object using minimal JSON (instead of inserting newlines and
  indenting for readability)

`-s`
  Accepted for backwards compatibility only.

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Fetch the object with key "lisa" in bucket "accounts":

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

## SEE ALSO

`moray(1)`, `putbucket(1)`, `putobject(1)`, `findobjects(1)`
