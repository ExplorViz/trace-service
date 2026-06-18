# trace-service

The trace-service is a scalable service that processes, interprets, persists, and queries [OpenTelemetry execution traces](https://opentelemetry.io/docs/concepts/signals/traces/) within monitored software applications. It attempts to classify the entities described by incoming spans for the purpose of visualization.

Consumes traces sent via Kafka from an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) instance (defined in the [Deployment](../deployment)). Processed spans are produced to a Kafka topic for the [Landscape Service](../landscape-service) to consume. Interaction with Kafka is implemented using the [franz-go](https://github.com/twmb/franz-go) library.

For development instructions, continue reading below. If you just want to run ExplorViz locally, refer to our [Deployment repository](../deployment) instead.

## Development Instructions

### Prerequisites

- Go 1.25.10 or higher
- A code editor, such as [Visual Studio Code](https://code.visualstudio.com/)
- Make sure to run the [ExplorViz software stack](../deployment)
  before starting the service, as it provides the required database(s) and the Kafka broker

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

### Compiling Protobuf

When updating any `.proto` files, make sure to compile the Protobuf files to Go using:

```shell
go generate
```

Alternatively, you can use the provided Makefile to compile the Protobuf and build the project in a single step:

```shell
make
```

If you just want to run the project while also compiling the Protobuf, use:

```shell
make run
```

### Code Style

As part of our CI/CD pipeline, your code is linted and checked for formatting using [golangci-lint](https://github.com/golangci/golangci-lint), which you can also install locally to lint your code yourself prior to pushing. We recommend using the [official Visual Studio Code extension for Go](https://marketplace.visualstudio.com/items?itemName=golang.go) as well as [configuring the extension for golangci-lint](https://golangci-lint.run/docs/welcome/integrations/) to detect and fix linting / formatting issues as you're working.
