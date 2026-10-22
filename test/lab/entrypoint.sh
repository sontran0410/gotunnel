#!/bin/sh
# Starts sshd plus the services of one lab role: jump1, jump2 or dest.
set -e
install -d -m 700 -o tunnel -g tunnel /home/tunnel/.ssh
install -m 600 -o tunnel -g tunnel /lab/id_ed25519.pub /home/tunnel/.ssh/authorized_keys

case "$1" in
jump1)
	# Only reachable with TLS, like a gateway that needs "openssl s_client" as ProxyCommand.
	socat OPENSSL-LISTEN:443,cert=/lab/tls.pem,verify=0,reuseaddr,fork TCP:127.0.0.1:22 &
	;;
dest)
	# Test services, reachable only from inside the destination network.
	socat TCP-LISTEN:7000,reuseaddr,fork EXEC:cat &
	mkdir -p /srv/www
	[ -f /srv/www/big.bin ] || head -c 20971520 /dev/urandom > /srv/www/big.bin
	sha256sum /srv/www/big.bin | cut -d' ' -f1 > /srv/www/big.sha256
	httpd -p 8080 -h /srv/www
	# Shell noise on login, which the agent preamble must survive.
	echo 'echo "Welcome to dest"' >> /home/tunnel/.bashrc
	;;
esac
exec /usr/sbin/sshd -D -e
