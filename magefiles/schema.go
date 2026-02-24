package main

import (
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// Schema contains targets for installing database schemas.
type Schema mg.Namespace

// CassEs installs Cassandra and Elasticsearch schemas.
func (s Schema) CassEs() error {
	mg.Deps(Build.CassandraTool)
	color("Install Cassandra schema...")
	cmds := [][]string{
		{"./temporal-cassandra-tool", "drop", "-k", temporalDB, "-f"},
		{"./temporal-cassandra-tool", "create", "-k", temporalDB, "--rf", "1"},
		{"./temporal-cassandra-tool", "-k", temporalDB, "setup-schema", "-v", "0.0"},
		{"./temporal-cassandra-tool", "-k", temporalDB, "update-schema", "-d", "./schema/cassandra/temporal/versioned"},
	}
	for _, cmd := range cmds {
		if err := sh.RunV(cmd[0], cmd[1:]...); err != nil {
			return err
		}
	}

	return s.Es()
}

// Mysql8 installs MySQL 8 schemas for temporal and visibility databases.
func (Schema) Mysql8() error {
	mg.Deps(Build.SqlTool)
	color("Install MySQL schema...")
	tool := "./temporal-sql-tool"
	baseArgs := []string{"-u", sqlUser, "--pw", sqlPassword, "--pl", "mysql8"}

	// Temporal DB
	for _, action := range [][]string{
		{"--db", temporalDB, "drop", "-f"},
		{"--db", temporalDB, "create"},
		{"--db", temporalDB, "setup-schema", "-v", "0.0"},
		{"--db", temporalDB, "update-schema", "-d", "./schema/mysql/v8/temporal/versioned"},
	} {
		args := append(baseArgs, action...)
		if err := sh.RunV(tool, args...); err != nil {
			return err
		}
	}

	// Visibility DB
	for _, action := range [][]string{
		{"--db", visibilityDB, "drop", "-f"},
		{"--db", visibilityDB, "create"},
		{"--db", visibilityDB, "setup-schema", "-v", "0.0"},
		{"--db", visibilityDB, "update-schema", "-d", "./schema/mysql/v8/visibility/versioned"},
	} {
		args := append(baseArgs, action...)
		if err := sh.RunV(tool, args...); err != nil {
			return err
		}
	}
	return nil
}

// Postgresql12 installs PostgreSQL 12 schemas for temporal and visibility databases.
func (Schema) Postgresql12() error {
	mg.Deps(Build.SqlTool)
	color("Install Postgres schema...")
	tool := "./temporal-sql-tool"
	baseArgs := []string{"-u", sqlUser, "--pw", sqlPassword, "-p", "5432", "--pl", "postgres12"}

	// Temporal DB
	for _, action := range [][]string{
		{"--db", temporalDB, "drop", "-f"},
		{"--db", temporalDB, "create"},
		{"--db", temporalDB, "setup", "-v", "0.0"},
		{"--db", temporalDB, "update-schema", "-d", "./schema/postgresql/v12/temporal/versioned"},
	} {
		args := append(baseArgs, action...)
		if err := sh.RunV(tool, args...); err != nil {
			return err
		}
	}

	// Visibility DB
	for _, action := range [][]string{
		{"--db", visibilityDB, "drop", "-f"},
		{"--db", visibilityDB, "create"},
		{"--db", visibilityDB, "setup-schema", "-v", "0.0"},
		{"--db", visibilityDB, "update-schema", "-d", "./schema/postgresql/v12/visibility/versioned"},
	} {
		args := append(baseArgs, action...)
		if err := sh.RunV(tool, args...); err != nil {
			return err
		}
	}
	return nil
}

// Es installs Elasticsearch schema.
func (Schema) Es() error {
	mg.Deps(Build.EsTool)
	color("Install Elasticsearch schema...")
	tool := "./temporal-elasticsearch-tool"
	if err := sh.RunV(tool, "-ep", "http://127.0.0.1:9200", "setup-schema"); err != nil {
		return err
	}
	return sh.RunV(tool, "-ep", "http://127.0.0.1:9200", "create-index", "--index", "temporal_visibility_v1_dev")
}

// EsSecondary installs secondary Elasticsearch schema.
func (Schema) EsSecondary() error {
	mg.Deps(Build.EsTool)
	color("Install Elasticsearch schema...")
	tool := "./temporal-elasticsearch-tool"
	if err := sh.RunV(tool, "-ep", "http://127.0.0.1:8200", "setup-schema"); err != nil {
		return err
	}
	return sh.RunV(tool, "-ep", "http://127.0.0.1:8200", "create-index", "--index", "temporal_visibility_v1_secondary")
}

// Xdc installs XDC (cross-datacenter) schemas.
func (Schema) Xdc() error {
	mg.Deps(Build.CassandraTool, Build.EsTool)
	cassTool := "./temporal-cassandra-tool"
	esTool := "./temporal-elasticsearch-tool"

	for _, cluster := range []string{"temporal_cluster_a", "temporal_cluster_b", "temporal_cluster_c"} {
		color("Install Cassandra schema (%s)...", cluster)
		cmds := [][]string{
			{cassTool, "drop", "-k", cluster, "-f"},
			{cassTool, "create", "-k", cluster, "--rf", "1"},
			{cassTool, "-k", cluster, "setup-schema", "-v", "0.0"},
			{cassTool, "-k", cluster, "update-schema", "-d", "./schema/cassandra/temporal/versioned"},
		}
		for _, cmd := range cmds {
			if err := sh.RunV(cmd[0], cmd[1:]...); err != nil {
				return err
			}
		}
	}

	color("Install Elasticsearch schemas...")
	if err := sh.RunV(esTool, "-ep", "http://127.0.0.1:9200", "setup-schema"); err != nil {
		return err
	}
	for _, idx := range []string{
		"temporal_visibility_v1_dev_cluster_a",
		"temporal_visibility_v1_dev_cluster_b",
		"temporal_visibility_v1_dev_cluster_c",
	} {
		// Drop silently (may not exist).
		_ = sh.RunV(esTool, "-ep", "http://127.0.0.1:9200", "drop-index", "--index", idx, "--fail")
		if err := sh.RunV(esTool, "-ep", "http://127.0.0.1:9200", "create-index", "--index", idx); err != nil {
			return err
		}
	}
	return nil
}
