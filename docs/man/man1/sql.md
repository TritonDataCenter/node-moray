# sql 1 "October 2017" Moray "Moray Client Tools"

## NAME

sql - check if a Moray server is functioning

## SYNOPSIS

`sql [COMMON_OPTIONS] [-r] SQL`

## DESCRIPTION

Executes a raw SQL command `SQL` against Moray's backing data store.  This is
intended for experienced operators and developers to run ad hoc queries (usually
read-only).  A mistake in the `SQL` string can cause all manner of bad behavior,
including database deadlock, data corruption, and data loss.  This tool and
Moray perform almost no validation on the input string.

This should not be used as part of normal operation.  Normal operations should
be first-classed as specific Moray requests with associated tools.

## OPTIONS

`-r`
  *Only for use in electric-moray.*  Set the read-only override flag
  (readOnlyOverride) to allow queries to run even if the vnode a particular
  object hashes to has been marked read-only.  It is not possible in an
  arbitrary case to know the reason a particular vnode has been set to
  read-only, so it is important to note once again that *this is dangerous and
  should ONLY be used by operators and developers while debugging*.

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Run a simple SQL `SELECT` query for the current time:

    $ sql 'SELECT NOW()'
    {
      "now": "2017-01-27T17:12:42.651Z"
    }

## SEE ALSO

`moray(1)`

## BUGS

Moray removes newlines from all SQL commands that it executes, including those
specified with this command.  The results can be surprising.  This most commonly
affects SQL "--" comments, which run to the end of the line.  Since newlines are
stripped, everything after the first opening "--" gets commented out.
