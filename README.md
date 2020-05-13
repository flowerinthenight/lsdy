[![CircleCI](https://circleci.com/gh/flowerinthenight/lsdy/tree/master.svg?style=svg)](https://circleci.com/gh/flowerinthenight/lsdy/tree/master)

## Overview

`lsdy` is a tool for querying [DynamoDB](https://aws.amazon.com/dynamodb/) tables.

## Installation

Using [Homebrew](https://brew.sh/):
```bash
$ brew tap flowerinthenight/tap
$ brew install lsdy
```

If you have a Go environment:
```bash
$ go get -u -v github.com/flowerinthenight/lsdy
```

## Usage
For a more updated help information:
```bash
$ lsdy -h
```

To authenticate to AWS, this tool looks for the following environment variables (can be set by cmdline args as well):
```bash
AWS_REGION
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY

# Optional:
ROLE_ARN

# When ROLE_ARN is set, it will assume this role using the provided key/secret pair.
```
