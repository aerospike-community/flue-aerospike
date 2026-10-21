FROM aerospike-server:dev

RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends ca-certificates wget gnupg; \
	wget -qO- https://artifact.aerospike.io/artifactory/api/security/keypair/aerospike/public \
		| gpg --dearmor -o /usr/share/keyrings/aerospike.gpg; \
	. /etc/os-release; \
	arch="$(dpkg --print-architecture)"; \
	printf 'deb [arch=%s signed-by=/usr/share/keyrings/aerospike.gpg] https://artifact.aerospike.io/artifactory/deb %s main\n' \
		"$arch" "$VERSION_CODENAME" \
		> /etc/apt/sources.list.d/aerospike.list; \
	apt-get update; \
	apt-get install -y --no-install-recommends aerospike-aql aerospike-asadm; \
	rm -rf /var/lib/apt/lists/*
