package schedule_test

import (
	"testing"
	"time"

	"github.com/jobqueue/api/internal/schedule"
)

func TestFirstRunAndNextRun(t *testing.T) {
	loc := time.UTC
	from := time.Date(2026, 6, 23, 10, 0, 0, 0, loc)

	first, err := schedule.FirstRun("0 11 * * *", loc, from)
	if err != nil {
		t.Fatal(err)
	}
	if first.Hour() != 11 || first.Day() != 23 {
		t.Fatalf("first run: got %v", first)
	}

	next, err := schedule.NextRun("0 11 * * *", loc, first)
	if err != nil {
		t.Fatal(err)
	}
	if !next.After(first) {
		t.Fatalf("next should be after first: first=%v next=%v", first, next)
	}
	if next.Hour() != 11 {
		t.Fatalf("next run hour: got %v", next)
	}
}

func TestEveryInterval(t *testing.T) {
	loc := time.UTC
	from := time.Now().UTC()

	first, err := schedule.FirstRun("@every 100ms", loc, from)
	if err != nil {
		t.Fatal(err)
	}
	if first.Before(from) {
		t.Fatalf("first run in the past: %v", first)
	}
}

func TestInvalidCron(t *testing.T) {
	_, err := schedule.FirstRun("not valid", time.UTC, time.Now())
	if err == nil {
		t.Fatal("expected error for invalid cron")
	}
}

func TestLoadLocationDefaultUTC(t *testing.T) {
	loc, err := schedule.LoadLocation("")
	if err != nil {
		t.Fatal(err)
	}
	if loc != time.UTC {
		t.Fatalf("got %v", loc)
	}
}
