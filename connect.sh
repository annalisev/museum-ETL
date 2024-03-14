source .env
export PGPASSWORD=$DB_PASSWORD

psql --host $DB_HOST -U $DB_USER $DB_NAME