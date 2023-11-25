Build command  
`docker image build -t us-east4-docker.pkg.dev/avid-garage-399623/cis9440-final-project/nyc-311-service-request-pipeline:latest .`

Push command  
`docker push us-east4-docker.pkg.dev/avid-garage-399623/cis9440-final-project/nyc-311-service-request-pipeline:latest`

Build DataFlow template  
```
gcloud dataflow flex-template build gs://cis9440-kr-dataflow/dataflow/templates/nyc-311-service-request-pipeline.json --image-gcr-path "us-east4-docker.pkg.dev/avid-garage-399623/cis9440-final-project/nyc-311-service-request-pipeline:latest" --sdk-language "PYTHON" --flex-template-base-image "gcr.io/dataflow-templates-base/python39-template-launcher-base" --metadata-file "metadata.json" --py-path "." --env "FLEX_TEMPLATE_PYTHON_PY_FILE=pipeline-extract-311data.py" --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"
```