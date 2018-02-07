# Geb Client in Go

## Installation

1. Use an ssh agent to allow git clone without any user input
2. Add dependency to Gopkg.toml (see below)
3. run dep ensure

### Gopkg.toml

```toml
[[constraint]]
  name = "gitlab.com/proemergotech/geb-client-go"
  source = "git@gitlab.com:proemergotech/geb-client-go.git"
  version = "0.1.6"
```

## Usage

see [examples.go](examples.go)
