# gettokens 1 "January 2017" Moray "Moray Client Tools"

## NAME

gettokens - fetch the list of shards from electric-moray

## SYNOPSIS

`gettokens [COMMON_OPTIONS]`

## DESCRIPTION

Reports the list of physical shards behind an electric-moray instance.

Electric-moray is a component typically deployed in front of several separate
Moray shards.  Electric-moray instances speak the Moray protocol to their
clients, but they use consistent hashing on some field of each object to select
the appropriate backend to handle each request.  (This functions similar to a
layer-7 load balancer.)  A full discussion of electric-moray is beyond the scope
of this documentation.

For this command, the service specified with the `COMMON_OPTIONS` should be an
electric-moray instance, not a moray instance.

## OPTIONS

See `moray(1)` for information about the `COMMON_OPTIONS`, which control
the log verbosity and how to locate the remote server.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Using an electric-moray service, this operation reports a list of URLs:

    $ gettokens -h electric-moray.emy-10.joyent.us
    {
      "tokens": [
        "tcp://1.moray.emy-10.joyent.us:2020",
        "tcp://2.moray.emy-10.joyent.us:2020"
      ]
    }

Using most Moray servers, this operation is not supported:

    $ gettokens
    gettokens: moray client ("172.27.10.72:52387" to "172.27.10.76:2020"): request failed: server error: Operation not supported

## SEE ALSO

`moray(1)`

## BUGS

It's not clear why backend shards are called "tokens".

This command likely belongs with an electric-moray tool suite, rather than the
Moray tool suite.
