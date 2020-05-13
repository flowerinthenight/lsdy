[![CircleCI](https://circleci.com/gh/flowerinthenight/lsdy/tree/master.svg?style=svg)](https://circleci.com/gh/flowerinthenight/lsdy/tree/master)

## Overview

`lsdy` is a tool for querying [DynamoDB](https://aws.amazon.com/dynamodb/) tables. It will attempt to display the values in a tabular form using all available attributes (alphabetical order, left to right), unless specified. If `--pk` is specified, it will query the table with that specific primary key. For tables with sort keys, only string-based sort keys are supported at the moment. When the `--sk` flag supplied, it will query all sort keys that begins with the flag value. An empty primary key implies a table scan.

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

To query a table using a primary key:
```bash
# Query table with primary key 'id' value of 'ID0001':
$ lsdy TABLE_NAME --pk "id:ID0001"
```

To query a table using both a primary key and a sort key:
```bash
# Query table with primary key 'id' value of 'ID0001' and sort key 'sortkey' of SK002:
$ lsdy TABLE_NAME --pk "id:ID0001" --sk "sortkey:SK002"
```

To scan a table:
```bash
# All attributes (columns) will be queried:
$ lsdy TABLE_NAME

# If you want specified attributes:
$ lsdy TABLE_NAME --attr "col1,col2,col3"

# or you can write it this way:
$ lsdy TABLE_NAME --attr col1 --attr col2 attr col3
```

If you want to describe a table:
```bash
# Will output the table details and all its attributes/columns:
$ lsdy TABLE_NAME --describe
```
