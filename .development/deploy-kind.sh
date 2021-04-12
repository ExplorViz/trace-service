#!/bin/bash

./gradlew clean assemble -Dquarkus.container-image.build=true -Dquarkus.container-image.image=explorviz/trace-service-jvm:latest

kind load docker-image --name explorviz-dev explorviz/trace-service-jvm:latest

kubectl apply --context kind-explorviz-dev -f manifest.yml