# Geb Client in Go

## Installation

1. Use an ssh agent to allow git clone without any user input
2. Add dependency to glide.yaml (see below)
3. run glide update

### glide.yaml

```yaml
package: your-package
import:
  - package: gitlab.com/proemergotech/geb-client-go
    repo:    git@gitlab.com:proemergotech/geb-client-go.git
    vsc:     git
    version: ^0.1
```

## Usage

see (examples.go)[examples.go]
