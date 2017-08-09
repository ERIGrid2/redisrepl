# README #

This is a small command line program to replicate remotely the commands sent to a local Redis instance. The remote instance is assumed to be running behind a HTTPS interface, conforming to the [Webdis](http://webd.is/) syntax. Since Webdis only supports plain HTTP, an HTTPS<->HTTP proxy must be running in front of it. The HTTPS connection must use bidirectional authentication, so that the appropriate certificate files must be provided via command line parameters. It is optionally possible to specify a namespace (i.e. a string prefix) in the remote instance to be replicated locally.  
**Plase note that this program is not intended to be a reliable replication mechanism, the state of the local and of the remote instance will diverge in case of loss of connection.**  

# License #

See the COPYING file.
