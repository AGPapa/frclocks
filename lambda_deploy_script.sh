#!/bin/bash
rm -rf deployment
mkdir deployment
mkdir deployment/transforms
mkdir deployment/lookups
mkdir deployment/html_templates
mkdir deployment/html_templates/includes
cp *.py ./deployment/
cp transforms/*.sql ./deployment/transforms/
cp lookups/*.csv ./deployment/lookups/
cp html_templates/*.html ./deployment/html_templates/
cp html_templates/includes/*.html ./deployment/html_templates/includes/
cd deployment
zip -r ../lambda_deployment.zip .
cd ..

rm -rf layer
mkdir -p layer/python/lib/python3.13/site-packages
docker run --rm -v "$PWD":/var/task python:3.13 pip install -r /var/task/requirements.txt -t /var/task/layer/python/lib/python3.13/site-packages
find layer -type d \( -name "tests" -o -name "__pycache__" -o -name "docs" -o -name "*.dist-info" \) -exec rm -rf {} +
cd layer
zip -r ../layer.zip python
cd ..
