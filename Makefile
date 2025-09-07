
.PHONY: init run up down logs clean clean-db

init:
	PYTHONPATH=. python scripts/init_db.py

run:
	python -m flows.pipeline

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f db

clean:
	rm -rf reports/*

clean-db:
	docker compose exec db psql -U ifx -d ifx -c "TRUNCATE TABLE raw_carpark_current_availability, ref_carpark_info, raw_carpark_availability_6pm_last_30days;"
