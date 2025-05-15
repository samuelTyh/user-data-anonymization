.PHONY: help build run stop clean

# Configuration variables
IMAGE_NAME := anonymization-pipeline
CONTAINER_NAME := app
DATA_DIR := ./data

help:
	@echo "User Data Anonymization Pipeline"
	@echo ""
	@echo "Available targets:"
	@echo "  help       - Show this help message"
	@echo "  build      - Build the Docker deployment image"
	@echo "  run        - Run the pipeline in a Docker container"
	@echo "  test       - Run all tests using the test image"
	@echo "  stop       - Stop the running container"
	@echo "  clean      - Remove the container and image"
	@echo ""

$(DATA_DIR):
	@echo "Creating data directory: $(DATA_DIR)"
	mkdir -p $(DATA_DIR)

# Build the Docker image (deployment image by default)
build:
	@echo "Building Docker image: $(IMAGE_NAME)"
	docker build --target deploy -t $(IMAGE_NAME) .

# Run the pipeline with default parameters
run: $(DATA_DIR) clean build
	@echo "Running pipeline in Docker container: $(CONTAINER_NAME)"
	docker run --name $(CONTAINER_NAME) \
		-v $(PWD)/$(DATA_DIR):/app/data \
		$(IMAGE_NAME)

# Stop the running container
stop:
	@echo "Stopping container: $(CONTAINER_NAME)"
	docker stop $(CONTAINER_NAME) || true

# Clean up containers and images
clean: stop
	@echo "Removing container: $(CONTAINER_NAME)"
	docker rm $(CONTAINER_NAME) || true
	@echo "Removing image: $(IMAGE_NAME)"
	docker rmi $(IMAGE_NAME) || true
	docker rmi $(IMAGE_NAME)-test || true

# Run tests in Docker using the test image
test:
	@echo "Building test image: $(IMAGE_NAME)-test"
	docker build --target test -t $(IMAGE_NAME)-test .
	@echo "Running tests in Docker container"
	docker run --rm \
		-v $(PWD)/$(DATA_DIR):/app/data \
		$(IMAGE_NAME)-test