# moraylisten 1 "June 2020" Moray "Moray Client Tools"

## NAME

moraylisten - listen for Moray notifications on the given channel

## SYNOPSIS

`moraylisten [COMMON_OPTIONS] CHANNEL`

## DESCRIPTION

Listens for notifications on `CHANNEL` and outputs the notification payload
(string) to stdout. This is a long process and can be stopped using a SIGINT
interrupt (Ctrl-C).

## ENVIRONMENT

See `moray(1)` for information about the `LOG_LEVEL`, `MORAY_SERVICE`, and
`MORAY_URL` environment variables.

## EXAMPLES

Listen for status notifications, the payload is an arbitrary string:

    $ moraylisten status
    Connected - listening for notifications
    online 2020-06-22T23:17:36.689Z
    offline 2020-06-22T23:19:16.164Z
    online 2020-06-22T23:19:21.762Z
    offline 2020-06-22T23:50:00.000Z
    ^C Caught interrupt signal - shutting down

Listen for workflow notifications, the payload in this case is an encoded JSON
string:

    $ moraylisten workflow_job_status_changed | json -ga
    Connected - listening for notifications
    {
        "prevExecution": null,
        "lastResult": {
            "result": "OK",
            "error": "",
            "name": "cnapi.release_vm_ticket",
            "started_at": "2020-06-22T20:31:27.853Z",
            "finished_at": "2020-06-22T20:31:28.049Z"
        },
        "name": "start-7.0.8",
        "execution": "succeeded",
        "uuid": "9093c11f-7034-4df4-9339-5f167fe37e9e"
    }
    ^C Caught interrupt signal - shutting down

## SEE ALSO

`moray(1)`, `moraynotify(1)`
