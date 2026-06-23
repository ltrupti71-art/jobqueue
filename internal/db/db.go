package db

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	sql, err := migrationFS.ReadFile("migrations/001_init.sql")
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}
	for _, stmt := range splitSQL(string(sql)) {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration: %w", err)
		}
	}
	return nil
}

func splitSQL(sql string) []string {
	var stmts []string
	for _, part := range strings.Split(sql, ";") {
		if s := strings.TrimSpace(part); s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}
