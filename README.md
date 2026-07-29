# trace-service

The trace-service is a scalable service that queries [OpenTelemetry execution traces](https://opentelemetry.io/docs/concepts/signals/traces/) gathered from monitored software applications. It provides a REST API for communication with the [frontend](https://github.com/ExplorViz/frontend). The trace-service is responsible for providing data regarding communcation between entities, including timestamps and detailed trace information.

Traces are queried from a [ClickHouse](https://github.com/clickhouse/clickhouse) database instance, which receives its data from our [custom OTel Collector](https://github.com/ExplorViz/otel-collector) via the ClickHouse exporter. The [custom schema](https://github.com/ExplorViz/deployment/tree/main/docker/compose/configurations/clickhouse) based on the ClickHouse exporter's default schema for traces with some materialized columns for efficient attribute access.

For development instructions, continue reading below. If you just want to run ExplorViz locally, refer to our [Deployment repository](https://github.com/ExplorViz/deployment) instead.

## Development Instructions

### Prerequisites

- Go 1.25.10 or higher
- A code editor, such as [Visual Studio Code](https://code.visualstudio.com/)
- Make sure to run the [ExplorViz software stack](https://github.com/ExplorViz/deployment)
  before starting the service, as it provides the required database(s) and the Collector instance

### Running the service

You can run the service using:

```shell
go run . [OPTIONS]
```

To see a list of command-line options, use the `--help` flag. These options can also be configured via environment variables, where the name of the environment variable corresponds to the long flag name, prefixed by `EXPLORVIZ_` and with all separators replaced by underscores; for example, the `--log-level` flag corresponds to the `EXPLORVIZ_LOG_LEVEL` environment variable. Note that directly passing flags takes precedence over environment variables. If neither the flag nor the environment variable is set, then the default value indicated by `--help` is used.

### Building an executable

To build an executable from the project, use:

```shell
go build
```

By default, the executable will be placed in the root directory under the name `trace-service`. You can optionally specify the path of the resulting binary using the `-o <your-executable-name>` flag.

### Testing

Be sure to write tests for new code and ensure that existing tests pass. You can run all tests using:

```shell
go test ./...
```

### Installing Git hooks

This repository provides Git hooks to verify your code prior to pushing. These can be installed via the included script by running:

```shell
go generate ./...
```

### Code Style

As part of our CI/CD pipeline, your code is linted and checked for formatting using [golangci-lint](https://github.com/golangci/golangci-lint), which you can also install locally to lint your code yourself prior to pushing. We recommend using the [official Visual Studio Code extension for Go](https://marketplace.visualstudio.com/items?itemName=golang.go) as well as [configuring the extension for golangci-lint](https://golangci-lint.run/docs/welcome/integrations/) to detect and fix linting / formatting issues as you're working.
