# morayversion 1 "January 2017" Moray "Moray Client Tools"

## NAME

morayversion - report the version of a Moray server

## SYNOPSIS

`morayversion [COMMON_OPTIONS]`

## DESCRIPTION

Attempts to determine the major version of the Moray server.  **This information
should not be used programmatically.**  This command is intended primarily for
operators and developers to use against specific Moray instances, either for
debugging or in preparation for upgrading.

There are important caveats about this operation:

- It's always possible for an operator to upgrade or rollback a Moray server
  immediately after this command is executed.  It's not safe to assume that the
  value returned by this command will not change over time.
- With most forms of service discovery (described in `moray(1)`), this request
  may be made against any of a number of different Moray server instances.  But
  different Moray instances may be running different versions.  This case
  further demonstrates that it's not safe to assume that the value returned by
  this command will not change over time, even from one millisecond to the next.
- Old versions of Moray do not respond to the version request at all.  On such
  servers, this command will time out (currently after 20 seconds).  It's
  impossible to know if a timeout indicates an old version, a bug or hang on the
  remote server, or a network issue.

These caveats generally make this command unsuitable for programmatic use.

## OPTIONS

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Check the version of a Moray server:

    $ morayversion
    2

## SEE ALSO

`moray(1)`
