# findobjects 1 "January 2017" Moray "Moray Client Tools"

## NAME

findobjects - fetch the contents of multiple objects using a filter

## SYNOPSIS

`findobjects [COMMON_OPTIONS] [-Hins] [-a FIELD] [-d FIELD] [-l LIMIT] [-o OFFSET] BUCKET FILTER`

## DESCRIPTION

Fetches objects from bucket `BUCKET` whose properties match the filter `FILTER`.
Matching objects are printed as JSON objects similar to the `getobject` command,
with properties `bucket`, `key`, `value`, `_id`, and `_etag`.  This command also
provides an additional JSON property on each object:

`_count`
  A count of total objects matching this filter on the server.  See the
  important caveats below.

`findobjects` operations are always bounded in size.  See the `-l LIMIT` option.
You must use multiple invocations using the `-l LIMIT` and `-o OFFSET` options
(called _pagination_) to process arbitrarily large sets of objects.

`FILTER` is an LDAP-like filter string.  For example, this filter matches
objects having property "hostname" with value "wormhole":

    (hostname=wormhole)

This filter matches objects having a "unix\_timestamp" property at most
1482438844:

    (unix_timestamp<=1482438844)

This filter matches objects meeting both constraints:

    (&(hostname=wormhole)(unix_timestamp<=1482438844))

And this filter matches objects meeting either one:

    (|(hostname=wormhole)(unix_timestamp<=1482438844))

In order to avoid expensive table scans, Moray requires that filter strings use
at least one of the bucket's indexed fields in a way that could prune results
(e.g., not just on one side of an "OR" clause).  Note that you can still
construct filters that require table scans (e.g., `(hostname=*)`), and
reasonable-looking filters can still behave pathologically.  Performance depends
significantly on the behavior of the underlying data store.  The requirement
around indexed fields is a heuristic to prevent obviously-pathological behavior,
not a guarantee of good behavior.

Filters can include both indexed and unindexed fields, but **using unindexed
fields in filters is strongly discouraged.**  They cannot be used correctly with
pagination (the `-o OFFSET` and `-l LIMIT` options) and they do not produce
correct values for `_count`.  Attempting to use these options with filters that
use unindexed fields often results in truncated result sets.


## OPTIONS

`-a FIELD`
  Sort results in ascending order by indexed field `FIELD`.

`-d DESC`
  Sort results in descending order by indexed field `FIELD`.

`-H`
  Print objects using minimal JSON with one object per line (instead of
  inserting newlines within objects and indenting for readability)

`-i`
  Require all fields used in `FILTER` to have associated usable indexes.  That
  is, every field mentioned in `FILTER` must have a corresponding index, and no
  field must be being re-indexed.  If the requirement is not met, the command
  will return an error.

`-l LIMIT`
  Return at most `LIMIT` objects.  With `-o OFFSET`, this can be used to page
  through a large result set.  However, this interacts badly with filters on
  undexed fields as described above.  If this option is unspecified, a default
  limit is provided (which is currently 1000).

`-n`
  Do not report the `_count` field with each object.  (This also skips
  executing the underlying query on the server to perform the count.)

`-o OFFSET`
  Skip the first `OFFSET` objects matching the filter.  With `-l LIMIT`, this
  can be used to page through a large result set.

`-s`
  Do not execute the query, but instead report the SQL strings that would be
  used to execute the query on the underlying data store.

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Fetch objects in "accounts" having a non-NULL value of `loginName`:

    $ findobjects accounts '(loginName=*)'

Fetch objects in "accounts" having a numeric uid at most 800:

    $ findobjects accounts '(uid<=800)'

Run the same query, sorting the results in increasing order of uid:

    $ findobjects -a uid accounts '(uid<=800)'

Run the above query, fetching only the first 5 results:

    $ findobjects -l 5 accounts '(uid<=800)'

Run the above query, fetching the second 10 results:

    $ findobjects -o 5 -l 10 accounts '(uid<=800)'

Fetch the SQL that would be used to execute the previous query:

    $ findobjects -s -o 5 -l 10 accounts '(uid<=800)'
    {
      "count": "SELECT count(1) over () as _count, '7e99cfd6-9758-64a1-e20a-e4594bfb43be' AS req_id FROM accounts WHERE  ( uid <= $1 AND uid IS NOT NULL )  LIMIT 10 OFFSET 5",
      "query": "SELECT *, '7e99cfd6-9758-64a1-e20a-e4594bfb43be' AS req_id FROM accounts WHERE  ( uid <= $1 AND uid IS NOT NULL )  LIMIT 10 OFFSET 5",
      "args": [
        800
      ]
    }

## SEE ALSO

`moray(1)`, `putbucket(1)`, `putobject(1)`, `getobject(1)`, `delmany(1)`,
`updatemany(1)`.

## BUGS

For the reasons mentioned above, it is generally considered a bug that Moray
allows filter strings to use unindexed fields.  This is likely to change in
future versions.
