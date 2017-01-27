# updatemany 1 "January 2017" Moray "Moray Client Tools"

## NAME

updatemany - update multiple objects using a filter

## SYNOPSIS

`updatemany [COMMON_OPTIONS] [-d DATA] [-l LIMIT] BUCKET FILTER`

## DESCRIPTION

Updates objects from bucket `BUCKET` whose properties match the filter `FILTER`.
`DATA` is a JSON object describing what to update.  Properties of `DATA` must be
indexed fields.  For each matching row, the fields specified in `DATA` will be
updated to their corresponding values in `DATA`.  Other fields are unchanged.

Like `findobjects`, `updatemany` operations are always bounded in size.  See the
`-l LIMIT` option.  You must use multiple invocations to update arbitrarily
large lists of objects.

`FILTER` is an LDAP-like filter string described in `findobjects(1)`.  The
caveats described there around the use of unindexed fields apply to filters used
with `updatemany` as well.

## OPTIONS

`-d DATA`
  Specifies the fields to update in each matching object.

`-l LIMIT`
  Remove at most `LIMIT` objects.  This interacts badly with filters on
  unindexed fields, as described in `findobjects(1)`.  If this option is
  unspecified, a default limit is provided (which is currently 1000).

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Update objects in bucket "accounts" having a value of "country" equal to "UK"
so that the "country" is now "United Kingdom":

    $ updatemany -d '{ "country": "United Kingdom" }' accounts '(country=UK)'
    { count: 1, etag: 'ue6d321d' }

## SEE ALSO

`moray(1)`, `putbucket(1)`, `putobject(1)`, `findobjects(1)`
