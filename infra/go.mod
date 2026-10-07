// Keeps `go build ./...` and `go test ./...` out of this folder: it holds the CDK app, and its
// node_modules contain Go template files that would otherwise be picked up as packages.
module vault/infra
