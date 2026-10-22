package service

import (
	"strings"
	"testing"
)

func TestPlist(t *testing.T) {
	p := plist("local.gotunnel.client", Spec{
		Args:    []string{"/opt/gotunnel", "up", "-c", "/Users/me/cfg & co.yaml"},
		Env:     map[string]string{"PATH": "/opt/homebrew/bin:/usr/bin"},
		LogPath: "/Users/me/.gotunnel/logs/client.out",
	})
	for _, want := range []string{
		"<string>local.gotunnel.client</string>",
		"<string>/Users/me/cfg &amp; co.yaml</string>",
		"<key>SuccessfulExit</key>\n    <false/>",
		"<key>PATH</key>\n    <string>/opt/homebrew/bin:/usr/bin</string>",
		"<key>StandardErrorPath</key>",
	} {
		if !strings.Contains(p, want) {
			t.Errorf("plist lacks %q:\n%s", want, p)
		}
	}
}

func TestUnitFile(t *testing.T) {
	u := unitFile(Spec{
		Description: "gotunnel agent",
		Args:        []string{"/home/me/.gotunnel/bin/gotunnel", "agent", "run", "--idle-exit", "0s"},
		Env:         map[string]string{"GOTUNNEL_HOME": "/home/me/my tunnel"},
	})
	for _, want := range []string{
		"ExecStart=/home/me/.gotunnel/bin/gotunnel agent run --idle-exit 0s\n",
		`Environment="GOTUNNEL_HOME=/home/me/my tunnel"`,
		"Restart=on-failure",
		"WantedBy=default.target",
	} {
		if !strings.Contains(u, want) {
			t.Errorf("unit lacks %q:\n%s", want, u)
		}
	}
}

func TestCrontabMarker(t *testing.T) {
	tab := "0 * * * * backup\n@reboot /x agent start # gotunnel:agent\n"
	if got := withoutMarker(tab, "agent"); got != "0 * * * * backup\n" {
		t.Fatalf("withoutMarker = %q", got)
	}
	if got := shellQuote("it's here"); got != `'it'\''s here'` {
		t.Fatalf("shellQuote = %s", got)
	}
}
