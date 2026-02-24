package main

import (
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Server contains targets for starting the temporal server with various configurations.
type Server mg.Namespace

func startServer(configFile string) error {
	mg.Deps(Build.Server)
	return sh.RunV("./temporal-server", "--config-file", configFile, "--allow-no-auth", "start")
}

// Sqlite starts the server with SQLite configuration.
func (Server) Sqlite() error {
	return startServer("config/development-sqlite.yaml")
}

// SqliteFile starts the server with SQLite file-based configuration.
func (Server) SqliteFile() error {
	return startServer("config/development-sqlite-file.yaml")
}

// CassEs starts the server with Cassandra+Elasticsearch configuration.
func (Server) CassEs() error {
	return startServer("config/development-cass-es.yaml")
}

// CassEsDual starts the server with Cassandra+ES dual visibility configuration.
func (Server) CassEsDual() error {
	return startServer("config/development-cass-es-dual.yaml")
}

// CassEsCustom starts the server with custom Cassandra+ES configuration.
func (Server) CassEsCustom() error {
	return startServer("config/development-cass-es-custom.yaml")
}

// EsFi starts the server with ES fault injection configuration.
func (Server) EsFi() error {
	return startServer("config/development-cass-es-fi.yaml")
}

// Mysql8 starts the server with MySQL 8 configuration.
func (Server) Mysql8() error {
	return startServer("config/development-mysql8.yaml")
}

// MysqlEs starts the server with MySQL+ES configuration.
func (Server) MysqlEs() error {
	return startServer("config/development-mysql-es.yaml")
}

// Postgres12 starts the server with PostgreSQL 12 configuration.
func (Server) Postgres12() error {
	return startServer("config/development-postgres12.yaml")
}

// XdcClusterA starts XDC cluster A.
func (Server) XdcClusterA() error {
	return startServer("config/development-cluster-a.yaml")
}

// XdcClusterB starts XDC cluster B.
func (Server) XdcClusterB() error {
	return startServer("config/development-cluster-b.yaml")
}

// XdcClusterC starts XDC cluster C.
func (Server) XdcClusterC() error {
	return startServer("config/development-cluster-c.yaml")
}
