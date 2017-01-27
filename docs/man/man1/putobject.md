# putobject 1 "January 2017" Moray "Moray Client Tools"

## NAME

putobject - create or update an object

## SYNOPSIS

`putobject [COMMON_OPTIONS] [-d DATA] [-e ETAG] BUCKET KEY`

## DESCRIPTION

Creates or updates an object in bucket `BUCKET` with primary key `KEY`.  If no
object exists having the same key, a new object is created.  If the object
exists (and if its etag matches `ETAG`, if `-e` was specified), then the old
object is overwritten and atomically replaced with the new one.  The object's
contents are specified by the JSON string `DATA`.

## OPTIONS

`-d DATA`
  Specifies the contents of the object as a JSON string.  Generally, objects
  should conform to a user-defined schema, having a fixed set of properties
  and types.  However, there are no constraints imposed by the server other
  than uniqueness for fields that are specifically marked unique in the bucket
  configuration.  Top-level properties of the object that correspond to
  indexed fields will be extracted and included in an index so that it can be
  queried and updated.  See `putbucket(1)` for details.

`-e ETAG`
  Only execute this operation if the corresponding object on the server
  currently has etag `ETAG`.  Etags are computed on the server based on
  object contents, and they are included in the results of any object fetch
  operation.  This option can be used to implement optimistic concurrency
  control (as a form of test-and-set operation).

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Inserts or updates an object with key "lisa" in bucket "accounts":

    $ putobject -d '{ "loginName": "lisa", "uid": "800", "country": "USA" }' \
        accounts lisa

See also the examples in `moray(1)`.

## SEE ALSO

`moray(1)`, `putbucket(1)`
