#!/bin/bash

echo "Building FIX Session Manager..."
mvn clean package -DskipTests

echo "Starting FIX Session Manager..."
java -jar target/fix-session-manager-1.0.0-SNAPSHOT.jar
