FROM postgres:14.5

LABEL org.opencontainers.image.source="https://github.com/ponylang/postgres"

RUN openssl req -new -x509 -days 3650 -nodes \
      -out /var/lib/postgresql/server.crt \
      -keyout /var/lib/postgresql/server.key \
      -subj '/CN=localhost' && \
    chmod 600 /var/lib/postgresql/server.key && \
    chown postgres:postgres /var/lib/postgresql/server.crt /var/lib/postgresql/server.key

COPY init-md5-user.sh /docker-entrypoint-initdb.d/

CMD ["postgres", "-c", "ssl=on", \
     "-c", "ssl_cert_file=/var/lib/postgresql/server.crt", \
     "-c", "ssl_key_file=/var/lib/postgresql/server.key"]
