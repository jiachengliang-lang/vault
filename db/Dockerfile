# Applies init.sql to a fresh database. Used on AWS, where there's no docker-entrypoint-initdb.d.
FROM postgres:16-alpine
COPY init.sql migrate.sh /db/
ENTRYPOINT ["/db/migrate.sh"]
