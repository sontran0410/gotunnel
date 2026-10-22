# Image for every host of the lab: jump1 (sshd behind TLS), jump2 and dest.
FROM alpine:3.22

RUN apk add --no-cache openssh-server socat openssl busybox-extras bash coreutils gzip cronie \
 && ssh-keygen -A \
 && adduser -D -s /bin/bash tunnel \
 && echo 'tunnel:*' | chpasswd -e \
 && mkdir -p /etc/ssh/sshd_config.d \
 && printf '%s\n' \
      'AllowTcpForwarding yes' \
      'PasswordAuthentication no' \
      'UseDNS no' \
      'ClientAliveInterval 15' \
      'PerSourcePenalties no' \
      'MaxStartups 100' > /etc/ssh/sshd_config.d/lab.conf

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
