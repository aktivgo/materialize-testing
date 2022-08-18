start-docker:
	cd mz-docker && docker-compose up -d

stop-docker:
	cd mz-docker && docker-compose down

restart-docker: stop-docker start-docker

start-producer:
	cd mz-service/producer && go run main.go

start-consumer:
	cd mz-service/consumer && go run main.go