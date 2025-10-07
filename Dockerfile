###############################################################################
# database image
# https://hub.docker.com/_/postgres
# https://github.com/docker-library/postgres
###############################################################################
FROM postgres:18 AS database

COPY <<EOF /docker-entrypoint-initdb.d/extensions.sql
CREATE EXTENSION IF NOT EXISTS citext;
EOF

USER postgres

CMD ["postgres"]

