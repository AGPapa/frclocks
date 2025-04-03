#!/bin/bash
rm -rf deployment
mkdir deployment
mkdir deployment/transforms
mkdir deployment/dcmp_transforms
mkdir deployment/dcmp_divisions_transforms
mkdir deployment/lookups
mkdir deployment/html_templates
mkdir deployment/html_templates/includes
mkdir deployment/static
mkdir deployment/static/css
mkdir deployment/static/images
cp *.py ./deployment/
cp transforms/*.sql ./deployment/transforms/
cp dcmp_transforms/*.sql ./deployment/dcmp_transforms/
cp dcmp_divisions_transforms/*.sql ./deployment/dcmp_divisions_transforms/
cp lookups/*.csv ./deployment/lookups/
cp html_templates/*.html ./deployment/html_templates/
cp html_templates/includes/*.html ./deployment/html_templates/includes/
cp static/css/style.css ./deployment/static/css/
cp static/images/* ./deployment/static/images/
cd deployment
zip -r ../lambda_deployment.zip .
cd ..

rm -rf layer
mkdir -p layer/python/lib/python3.13/site-packages
docker run --rm -v "$PWD":/var/task python:3.13 pip install --no-cache-dir -r /var/task/requirements.txt -t /var/task/layer/python/lib/python3.13/site-packages
find layer -type d \( -name "tests" -o -name "__pycache__" -o -name "*.dist-info" \) -exec rm -rf {} +
cd layer
zip -r ../layer.zip python
cd ..
