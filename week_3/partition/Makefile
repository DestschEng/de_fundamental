include .env

# Build all services
build:
	docker-compose build
	@echo "Build command executed"

# Start all services in detached mode
up:
	docker-compose --env-file .env up -d

# Stop all services
down:
	docker-compose --env-file .env down

# Restart all services
restart:
	make down && make up

# List all running containers
ps:
	docker ps

# Connect to PostgreSQL
to_psql:
	docker exec -ti postgresql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgresql:5432/${POSTGRES_DB}
#	docker exec -ti de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

# Connect to MySQL as regular user
to_mysql:
	docker exec -it mysql mysql --local-infile=1 -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

# Connect to MySQL as root user
to_mysql_root:
	docker exec -it mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}