#!/bin/bash
rm -rf deployment
mkdir deployment
mkdir deployment/transforms
mkdir deployment/transforms/district
mkdir deployment/transforms/dcmp
mkdir deployment/transforms/dcmp_divisions
mkdir deployment/lookups
mkdir deployment/html_templates
mkdir deployment/html_templates/includes
mkdir deployment/static
mkdir deployment/static/css
mkdir deployment/static/images
cp *.py ./deployment/
cp transforms/district/*.sql ./deployment/transforms/district/
cp transforms/dcmp/*.sql ./deployment/transforms/dcmp/
cp transforms/dcmp_divisions/*.sql ./deployment/transforms/dcmp_divisions/
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
