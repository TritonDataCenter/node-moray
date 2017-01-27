# morayping 1 "January 2017" Moray "Moray Client Tools"

## NAME

morayping - check if a Moray server is functioning

## SYNOPSIS

`morayping [COMMON_OPTIONS] [-dF]`

## DESCRIPTION

Attempts to determine whether Moray is functioning.  For this command, success
indicates that Moray is functioning and able to respond to a request.  Failure
indicates the command could not successfully connect and complete a request.

## OPTIONS

`-d`
  Attempt to determine whether Moray has a working connection to its
  underlying data store by making a "deep" ping request instead of a trivial
  request.

`-F`
  Block until at least one Moray server is found and a connection is
  established, then execute the ping request.  This is intended to
  programmatically wait for Moray to become available, although if the request
  fails (e.g., because of a network issue), the request is not retried, and in
  this case Moray may still be down.

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Check whether a Moray server is online:

    $ morayping

Check whether the target server has working connections to its underlying data
store:

    $ morayping -d

When the remote server is not online, you might get an error like this:

    $ morayping -h 127.0.0.1
    morayping: moray client "127.0.0.1:2020": failed to establish connection

## SEE ALSO

`moray(1)`
