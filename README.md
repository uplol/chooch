# chooch - An Amazing Project

Downloads files faster than wget/curl (in theory) using multiple connections. Chooch recycles the slowest connection and re-connects with a new and unique source port.

In environments where LACP or ECMP (hash-based load balancing across multiple physical interfaces) is implemented, a naive multi-stream downloader may not utilize all the bond interfaces if they re-use the same static connections. Chooch aims to solve this by recycling slow connections and ensuring new connections are made with a new unique source port. Doing this adds entropy to the bond's hashing algorithm which may improve total throughput if the connections are evenly distributed across the it's physical interfaces.

In the future, Chooch may be expanded to support multiple source IPs, or could include a utility to pre-determine the best source port set to use for a given machine/network configuration. However, this optimization would require the download server to be reachable using a static address and port, as the source IP is a common facet used in bond hashing algorithms.

### Features

- Uses multiple connections in parallel to download quicker
- Terminates the slowest connection and re-connects with a new client port.
- Pre-allocates the file on-disk

### Usage

```bash
USAGE:
    chooch [FLAGS] [OPTIONS] <url> <output>

FLAGS:
    -f, --force-overwrite    Overwrites existing output file if it already exists
    -h, --help               Prints help information
    -s, --skip-prealloc      Skips the pre-allocation of the target file
    -V, --version            Prints version information

OPTIONS:
    -i, --bind-ip <bind-ip>          Sets the IP address used to make outgoing connections.
    -c, --chunk-size <chunk-size>     [default: 32MB]
    -w, --workers <worker-count>      [default: 6]

ARGS:
    <url>       The URL you wish to download
    <output>    The output destination for this download
```

### Todo

- TLS Support
- Handle requests without content-length for unlimited streaming
- More options for HTTP requests (headers, method, etc)

PRs welcome :)
