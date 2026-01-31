up:
	sudo docker build -t minio_batch ./docker/minio
	sudo docker build -t grafana_batch ./docker/grafana
	sudo docker build -t trino_batch ./docker/trino
	sudo astro dev start --env .env
	sudo docker exec -it trino trino --file /etc/trino/init-tables.sql
	
down:
	sudo astro dev stop

push:
	git push -u origin main

restart:
	sudo astro dev restart --env .env