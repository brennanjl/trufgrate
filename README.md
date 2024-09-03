# Trufgrate

Trufgrate (Truflation-Migrate) is a basic tool for migrating Truflation streams. It contains two subcommands:
- `primitive`: migrates primitive streams
- `composed`: migrates composed streams.

**This is just made to be an example.**

## Example

To migrate all primitive UK streams, run

```shell
go run . primitive --schema ./new_schema.kf --rpc http://truf-rpc.com:8484 --private-key eth_private_key --primitive-file cpi_uk/primitive_sources.csv
```

For more in-depth useage information, run `go run . primitive -h` or `go run . composed -h`.

## TODOs

There are two TODOs in the code: a TODO to upload primitive data, and a TODO to upload composed weights. These can be found in `primitive.go` and `composed.go`, respectively.