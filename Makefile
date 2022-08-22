start-docker:
	cd mz-docker && docker-compose up -d

stop-docker:
	cd mz-docker && docker-compose down

restart-docker: stop-docker start-docker

start-cli:
	cd mz-docker && docker-compose run cli

start-producer:
	cd mz-service/producer && go run main.go

start-events-consumer:
	cd mz-service/consumer/events && go run main.go

start-triggers-consumer:
	cd mz-service/consumer/triggers && go run main.go