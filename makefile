up:
	sudo docker build -t minio_batch ./docker/minio
	sudo docker build -t grafana_batch ./docker/grafana
	sudo astro dev start --env .env

down:
	sudo astro dev stop

push:
	git push -u origin main

restart:
	sudo astro dev restart --env .env