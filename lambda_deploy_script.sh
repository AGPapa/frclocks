mkdir deployment
pip install -r requirements.txt --target ./deployment
cp *.py ./deployment/
cp transformations/*.sql ./deployment/
cp lookup_tables/*.csv ./deployment/
cp html_templates/*.html ./deployment/
cp requirements.txt ./deployment/
cd deployment
zip -r ../lambda_deployment.zip .