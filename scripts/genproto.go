//go:build ignore

// Command genproto generates the Go code required for this application from the .proto files
// within the proto directory of this repository. This is achieved by calling the protoc compiler,
// which must be installed for this script to work.
//
// The purpose of this dedicated script is to provide a cross-platform way to invoke the protoc command.
// As such, this command is not intended to be run directly and should instead be invoked using go:generate.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	files, err := filepath.Glob("./proto/*.proto")
	if err != nil {
		fmt.Printf("error matching glob pattern: %v\n", err)
		os.Exit(1)
	}

	if len(files) == 0 {
		fmt.Println("no matching files found")
		return
	}

	args := append([]string{"-I=./proto", "--go_out=.", "--go_opt=module=github.com/ExplorViz/trace-service"}, files...)

	cmd := exec.Command("protoc", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fmt.Printf("protobuf generation failed: %v\n", err)
		os.Exit(1)
	}
}
