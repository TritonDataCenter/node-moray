# moraynotify 1 "June 2020" Moray "Moray Client Tools"

## NAME

moraynotify - send a Moray notification on the given channel

## SYNOPSIS

`moraynotify [COMMON_OPTIONS] CHANNEL PAYLOAD`

## DESCRIPTION

Sends the notification payload to `CHANNEL`.

Note that the maximum length of the payload is restricted to 8000 bytes.

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Notify a status notification, the payload is an arbitrary string:

    $ moraynotify status "online 2020-06-22T23:17:36.689Z"
    $ moraynotify status "offline 2020-06-22T23:23:00.000Z"

Notify a json encoded payload:

    $ moraynotify mychannel '{"name": "data", "item": {"length": 2}}'

## SEE ALSO

`moray(1)`, `moraylisten(1)`
