# delobject 1 "January 2017" Moray "Moray Client Tools"

## NAME

delobject - delete an object by primary key

## SYNOPSIS

`delobject [COMMON_OPTIONS] BUCKET KEY`

## DESCRIPTION

Removes the object in bucket `BUCKET` having primary key `KEY`.  This command
fails if the object does not exist.

## OPTIONS

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Remove the object in "accounts" with key "hugh":

    $ delobject accounts hugh

## SEE ALSO

`moray(1)`, `putbucket(1)`, `putobject(1)`
