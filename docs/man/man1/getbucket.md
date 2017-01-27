# getbucket 1 "January 2017" Moray "Moray Client Tools"

## NAME

getbucket - print detailed information about one bucket

## SYNOPSIS

`getbucket [COMMON_OPTIONS] BUCKET`

## DESCRIPTION

Fetches a JSON representation for the configuration of the bucket `BUCKET`.
This representation includes information about the bucket's version and the
indexes defined for it.

## OPTIONS

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Fetch details about bucket "accounts":

    $ getbucket accounts
    {
      "name": "accounts",
      "index": {
        "loginName": {
          "type": "string",
          "unique": true
        },
        "uid": {
          "type": "number",
          "unique": true
        },
        "country": {
          "type": "string",
          "unique": false
        }
      },
      "pre": [],
      "post": [],
      "options": {
        "version": 2
      },
      "mtime": "2017-01-27T17:56:42.005Z"
    }

## SEE ALSO

`moray(1)`, `putbucket(1)`
