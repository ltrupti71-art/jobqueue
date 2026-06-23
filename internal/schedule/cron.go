package schedule

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Parser accepts standard 5-field cron: minute hour dom month dow.
var Parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// NextRun returns the first run strictly after from using expr in loc.
func NextRun(expr string, loc *time.Location, from time.Time) (time.Time, error) {
	sched, err := Parser.Parse(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression: %w", err)
	}
	return sched.Next(from.In(loc)), nil
}

// FirstRun returns the first run at or after from.
func FirstRun(expr string, loc *time.Location, from time.Time) (time.Time, error) {
	next, err := NextRun(expr, loc, from.Add(-time.Nanosecond))
	if err != nil {
		return time.Time{}, err
	}
	return next.UTC(), nil
}

// LoadLocation returns UTC for empty name.
func LoadLocation(name string) (*time.Location, error) {
	if name == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone: %w", err)
	}
	return loc, nil
}
