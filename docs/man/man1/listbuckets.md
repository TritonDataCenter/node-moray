# listbuckets 1 "January 2017" Moray "Moray Client Tools"

## NAME

listbuckets - print detailed information about all buckets

## SYNOPSIS

`listbuckets [COMMON_OPTIONS]`

## DESCRIPTION

Fetches a JSON representation for the configuration of all buckets on the remote
server.  See `getbucket(1)` for details about this representation.

## OPTIONS

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

List details about the buckets on a Moray server:

    $ listbuckets
    [
      {
        "name": "accounts",
        "index": {
          "loginName": {
            "type": "string",
            "unique": true
          }
        },
        "pre": [],
        "post": [],
        "options": {
          "version": 1
        },
        "mtime": "2017-01-27T17:22:32.288Z"
      },
      {
        "name": "accounts_withnames",
        "index": {
          "loginName": {
            "type": "string",
            "unique": true
          },
          "fullName": {
            "type": "string",
            "unique": false
          }
        },
        "pre": [],
        "post": [],
        "options": {
          "version": 2
        },
        "mtime": "2017-01-27T17:22:45.965Z"
      }
    ]

Using the `json(1)` tool, you can easily list just the buckets' names:

    $ listbuckets | json -ga name
    accounts
    accounts_withnames


## SEE ALSO

`moray(1)`, `getbucket(1)`, `putbucket(1)`,
[jsontool](https://github.com/trentm/json)
