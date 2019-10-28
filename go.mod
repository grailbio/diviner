module github.com/grailbio/diviner

go 1.13

require (
	github.com/aws/aws-sdk-go v1.25.19
	github.com/google/gofuzz v1.0.0
	github.com/grailbio/base v0.0.5
	github.com/grailbio/bigmachine v0.5.5
	github.com/grailbio/testutil v0.0.3
	github.com/kr/pty v1.1.8
	go.etcd.io/bbolt v1.3.3
	// TODO(marius): upgrade starlark.
	go.starlark.net v0.0.0-20190206224905-6afa1bba75f9
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
)
