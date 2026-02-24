package main

import "github.com/magefile/mage/sh"

// Docker contains targets for managing Docker dependencies.
type Docker struct{}

func dockerComposeFiles() []string {
	return []string{
		"-f", "./develop/docker-compose/docker-compose.yml",
		"-f", "./develop/docker-compose/docker-compose." + goos + ".yml",
	}
}

func dockerComposeCDCFiles() []string {
	return []string{
		"-f", "./develop/docker-compose/docker-compose.cdc.yml",
		"-f", "./develop/docker-compose/docker-compose.cdc." + goos + ".yml",
	}
}

// StartDeps starts Docker dependencies.
func (Docker) StartDeps() error {
	color("Starting Docker dependencies...")
	args := append([]string{"compose"}, dockerComposeFiles()...)
	args = append(args, "up")
	return sh.RunV("docker", args...)
}

// StopDeps stops Docker dependencies.
func (Docker) StopDeps() error {
	color("Stopping Docker dependencies...")
	args := append([]string{"compose"}, dockerComposeFiles()...)
	args = append(args, "down")
	return sh.RunV("docker", args...)
}

// StartDepsDual starts Docker dependencies with secondary ES.
func (Docker) StartDepsDual() error {
	args := append([]string{"compose"}, dockerComposeFiles()...)
	args = append(args, "-f", "./develop/docker-compose/docker-compose.secondary-es.yml", "up")
	return sh.RunV("docker", args...)
}

// StopDepsDual stops Docker dependencies with secondary ES.
func (Docker) StopDepsDual() error {
	args := append([]string{"compose"}, dockerComposeFiles()...)
	args = append(args, "-f", "./develop/docker-compose/docker-compose.secondary-es.yml", "down")
	return sh.RunV("docker", args...)
}

// StartDepsCDC starts Docker dependencies with CDC.
func (Docker) StartDepsCDC() error {
	args := append([]string{"compose"}, dockerComposeFiles()...)
	args = append(args, dockerComposeCDCFiles()...)
	args = append(args, "up")
	return sh.RunV("docker", args...)
}

// StopDepsCDC stops Docker dependencies with CDC.
func (Docker) StopDepsCDC() error {
	args := append([]string{"compose"}, dockerComposeFiles()...)
	args = append(args, dockerComposeCDCFiles()...)
	args = append(args, "down")
	return sh.RunV("docker", args...)
}
