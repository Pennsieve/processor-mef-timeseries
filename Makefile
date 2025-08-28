.PHONY: help test run

SERVICE_NAME  ?= "processor-post-timeseries"

.DEFAULT: help

help:
	@echo "Make Help for $(SERVICE_NAME)"
	@echo ""
	@echo "make run				- run the processor locally via docker-compose"

run:
	docker-compose -f docker-compose.yml down --remove-orphans
	docker-compose -f docker-compose.yml build
	docker-compose -f docker-compose.yml up --exit-code-from processor
