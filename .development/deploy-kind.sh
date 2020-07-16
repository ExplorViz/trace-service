#!/bin/bash

./gradlew quarkusBuild

docker build -f src/main/docker/Dockerfile.jvm -t explorviz/trace-service-jvm .

kind load docker-image explorviz/trace-service-jvm:latest

kubectl apply --namespace=explorviz-dev -f manifest.yml