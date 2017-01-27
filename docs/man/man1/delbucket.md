# delbucket 1 "January 2017" Moray "Moray Client Tools"

## NAME

delbucket - delete a bucket from Moray

## SYNOPSIS

`delbucket [COMMON_OPTIONS] BUCKET`

## DESCRIPTION

Removes the bucket called `BUCKET` from Moray.  Any objects contained in the
bucket will be removed permanently.  This command fails if the bucket does not
exist.

## OPTIONS

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Delete a bucket called "accounts":

    $ delbucket accounts

## SEE ALSO

`moray(1)`, `putbucket(1)`
