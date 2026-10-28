package config

import (
	"strings"
	"testing"
	"time"
)

func TestAppStartDeadlineOptionalAndIndependent(t *testing.T) {
	c := SandboxConfig{Timeouts: TimeoutsConfig{AppNotify: "1h"}}
	for _, raw := range []string{"", "0", "0s", "3s"} {
		c.Timeouts.AppStart = raw
		if err := c.Timeouts.validate(); err != nil {
			t.Fatal(raw, err)
		}
		want := time.Duration(0)
		if raw == "3s" {
			want = 3 * time.Second
		}
		if c.AppStartDeadline() != want || c.AppNotifyDeadline() != time.Hour {
			t.Fatalf("%q: app_start=%s app_notify=%s", raw, c.AppStartDeadline(), c.AppNotifyDeadline())
		}
	}
	for _, raw := range []string{"-1s", "nonsense"} {
		c.Timeouts.AppStart = raw
		if err := c.Timeouts.validate(); err == nil || !strings.Contains(err.Error(), "timeouts.app_start") {
			t.Fatal(raw, err)
		}
	}
	if DefaultAppStartDeadline != 0 {
		t.Fatal("compatibility constant reintroduced a deadline")
	}
}
