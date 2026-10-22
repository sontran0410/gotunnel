// Command gotunnel keeps many port forwards alive over a slow, multi-hop ssh
// path. The same binary runs on the client machine and on the destination.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/sontran0410/gotunnel/internal/version"
)

const usage = `gotunnel - persistent tunnels over a slow, multi-hop ssh path

Client (your machine):
  gotunnel up [-c config] [-d]       start the client (-d: in the background)
  gotunnel down                      stop the client
  gotunnel status [--json]           transport state, forwards and counters
  gotunnel logs [-f] [-n lines]      show the client log
  gotunnel reload                    re-read the configuration file
  gotunnel reconnect                 drop the link now; the session resumes on a new one
  gotunnel forward add NAME --listen ADDR --target ADDR [--mode always|on-demand] [--priority P]
  gotunnel forward rm NAME
  gotunnel reverse add NAME --remote-listen ADDR --target ADDR [--priority P]
  gotunnel reverse rm NAME
  gotunnel service install|uninstall|status [-c config]
                                     start at login and restart after crashes
  gotunnel install-agent [-c config] [--binary file]
                                     upload the agent to the destination through ssh
  gotunnel doctor [-c config]        check the setup and explain problems

Destination (normally started through ssh):
  gotunnel agent start|stop|status|logs
  gotunnel agent run [--idle-exit 0s] run agentd in the foreground (for service managers)
  gotunnel agent service install|uninstall|status
  gotunnel agent stdio [--standalone] relay between the ssh channel and agentd

  gotunnel version

The default configuration file is ~/.config/gotunnel/config.yaml.
`

// exitCode makes main exit with a code without printing anything.
type exitCode int

func (e exitCode) Error() string { return fmt.Sprintf("exit %d", int(e)) }

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "up":
		err = cmdUp(args)
	case "down":
		err = cmdDown(args)
	case "status":
		err = cmdStatus(args)
	case "logs":
		err = cmdLogs(args)
	case "reload":
		err = cmdReload(args)
	case "reconnect":
		err = cmdReconnect(args)
	case "forward", "forwards":
		err = cmdForward(args)
	case "reverse":
		err = cmdReverse(args)
	case "service":
		err = cmdService(args)
	case "install-agent":
		err = cmdInstallAgent(args)
	case "doctor":
		err = cmdDoctor(args)
	case "agent":
		err = cmdAgent(args)
	case "version", "--version", "-v":
		fmt.Printf("gotunnel %s %s/%s\n", version.Version, runtime.GOOS, runtime.GOARCH)
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	var code exitCode
	switch {
	case err == nil:
	case errors.As(err, &code):
		os.Exit(int(code))
	case errors.Is(err, flag.ErrHelp):
		os.Exit(2)
	default:
		fmt.Fprintln(os.Stderr, "gotunnel:", err)
		os.Exit(1)
	}
}

// newFlags returns a flag set that reports errors instead of exiting.
func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet("gotunnel "+name, flag.ContinueOnError)
	return fs
}

// parseNamed parses "NAME [flags]" or "[flags] NAME".
func parseNamed(fs *flag.FlagSet, args []string) (string, error) {
	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name, args = args[0], args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	if name == "" {
		name = fs.Arg(0)
	}
	if name == "" {
		return "", errors.New("a name is required")
	}
	return name, nil
}

func usageError(format string, args ...any) error {
	return fmt.Errorf(format+"\nrun 'gotunnel help' for usage", args...)
}
