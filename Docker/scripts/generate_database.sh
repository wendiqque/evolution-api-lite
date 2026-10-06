#!/bin/bash

source ./Docker/scripts/env_functions.sh

if [ "$DOCKER_ENV" != "true" ]; then
    export_env_vars
fi

DATABASE_PROVIDER=${DATABASE_PROVIDER:-postgresql}

if [[ "$DATABASE_PROVIDER" == "postgresql" || "$DATABASE_PROVIDER" == "mysql" ]]; then
    DATABASE_URL=${DATABASE_URL:-$DATABASE_CONNECTION_URI}
    DATABASE_CONNECTION_URI=${DATABASE_CONNECTION_URI:-$DATABASE_URL}
    export DATABASE_URL
    export DATABASE_CONNECTION_URI
    echo "Generating database for $DATABASE_PROVIDER"
    echo "Database URL: $DATABASE_URL"
    npm run db:generate
    if [ $? -ne 0 ]; then
        echo "Prisma generate failed"
        exit 1
    else
        echo "Prisma generate succeeded"
    fi
else
    echo "Error: Database provider $DATABASE_PROVIDER invalid."
    exit 1
fi
