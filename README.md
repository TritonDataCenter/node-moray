# Moray (node.js client SDK)

Repository: <git@git.joyent.com:node-moray.git>
Browsing: <https://mo.joyent.com/node-moray>
Who: Mark Cavage
Docs: <https://mo.joyent.com/docs/moray>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>


# Overview

This is the (node.js) client SDK for [Moray](https://mo.joyent.com/docs/moray).
For usage information, visit the Moray docs.


# Testing

To test this Moray client:

- Clone the Moray server repo.
- Use "make" to build the Moray server repo.
- Use "npm ln" to link your client repo into the server repo for the "moray"
  dependency (e.g., "cd /path/to/server/repo; npm ln /path/to/client/repo").
- Follow the instructions in the Moray server repo to test it.  Since it's using
  your client, this will exercise the test suite using your client.
