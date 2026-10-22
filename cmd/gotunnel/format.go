package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"gotunnel/internal/client"
)

func printStatus(w io.Writer, st *client.Status) {
	t := st.Transport
	fmt.Fprintf(w, "gotunnel %s · pid %d (%s) · up %s\n", st.Version, st.PID, st.Managed, age(st.Started))
	fmt.Fprintf(w, "config:     %s\n", st.Config)
	line := fmt.Sprintf("transport:  %s · %s for %s", strings.ToUpper(string(t.State)), t.Target, age(t.Since))
	if t.Session != "" {
		line += " · session " + t.Session
	}
	if t.AgentVersion != "" {
		line += " · agent " + t.AgentVersion
	}
	fmt.Fprintln(w, line)
	if t.Session != "" {
		fmt.Fprintf(w, "            rtt %s (min %s) · in flight %s of %s · queued %s · sent %s · received %s\n",
			millis(t.RTTMillis), millis(t.MinRTTMillis), bytesH(uint64(t.Inflight)), bytesH(uint64(t.InflightLimit)),
			bytesH(uint64(t.Queued)), bytesH(t.BytesOut), bytesH(t.BytesIn))
	}
	fmt.Fprintf(w, "            %d active connections · %d link losses · %d resumed · %d sessions\n",
		t.ActiveConns, t.LinkLosses, t.Resumes, t.Sessions)
	if t.LastError != "" {
		fmt.Fprintf(w, "last error: %s\n", t.LastError)
	}
	fmt.Fprintln(w)
	printListeners(w, st)
}

func printListeners(w io.Writer, st *client.Status) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if len(st.Forwards) > 0 {
		fmt.Fprintln(tw, "FORWARD\tLISTEN\tTARGET\tMODE\tPRIORITY\tCONNS\tTOTAL\tUP/DOWN\t")
		for _, f := range st.Forwards {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s/%s\t%s\n", f.Name, f.Listen, f.Target, f.Mode, f.Priority,
				f.Active, f.Total, bytesH(f.BytesUp), bytesH(f.BytesDown), listenProblem(f.Listening, f.Error))
		}
	}
	if len(st.Socks) > 0 {
		fmt.Fprintln(tw, "SOCKS\tLISTEN\t\t\tPRIORITY\tCONNS\tTOTAL\tUP/DOWN\t")
		for _, s := range st.Socks {
			fmt.Fprintf(tw, "%s\t%s\t\t\t%s\t%d\t%d\t%s/%s\t%s\n", s.Name, s.Listen, s.Priority,
				s.Active, s.Total, bytesH(s.BytesUp), bytesH(s.BytesDown), listenProblem(s.Listening, s.Error))
		}
	}
	if len(st.Reverse) > 0 {
		fmt.Fprintln(tw, "REVERSE\tREMOTE LISTEN\tTARGET\tSTATE\tPRIORITY\tCONNS\tTOTAL\tUP/DOWN\t")
		for _, r := range st.Reverse {
			problem := ""
			if r.Error != "" {
				problem = "! " + r.Error
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s/%s\t%s\n", r.Name, r.RemoteListen, r.Target, r.State, r.Priority,
				r.Active, r.Total, bytesH(r.BytesUp), bytesH(r.BytesDown), problem)
		}
	}
	if len(st.Forwards)+len(st.Socks)+len(st.Reverse) == 0 {
		fmt.Fprintln(tw, "no forwards configured")
	}
	tw.Flush()
}

func listenProblem(listening bool, err string) string {
	switch {
	case err != "":
		return "! " + err + " (retrying)"
	case !listening:
		return "! not listening"
	}
	return ""
}

func bytesH(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func millis(ms float64) string {
	if ms == 0 {
		return "-"
	}
	if ms < 10 {
		return fmt.Sprintf("%.1fms", ms)
	}
	return fmt.Sprintf("%.0fms", ms)
}

func age(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t).Round(time.Second)
	switch {
	case d < time.Minute:
		return d.String()
	case d < time.Hour:
		return d.Round(time.Minute).String()
	default:
		h := int(d.Hours())
		return fmt.Sprintf("%dh%dm", h, int(d.Minutes())-60*h)
	}
}
