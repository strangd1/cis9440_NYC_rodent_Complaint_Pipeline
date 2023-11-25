Build command  
`docker image build -t us-east4-docker.pkg.dev/avid-garage-399623/cis9440-final-project/nyc-restaurant-inspection-extract-pipeline:latest .`

Push command  
`docker push us-east4-docker.pkg.dev/avid-garage-399623/cis9440-final-project/nyc-restaurant-inspection-extract-pipeline:latest`

Build DataFlow template  
```
gcloud dataflow flex-template build gs://cis9440-kr-dataflow/dataflow/templates/nyc-restaurant-inspection-extract-pipeline.json --image-gcr-path "us-east4-docker.pkg.dev/avid-garage-399623/cis9440-final-project/nyc-restaurant-inspection-extract-pipeline:latest" --sdk-language "PYTHON" --flex-template-base-image "gcr.io/dataflow-templates-base/python39-template-launcher-base" --metadata-file "metadata.json" --py-path "." --env "FLEX_TEMPLATE_PYTHON_PY_FILE=pipeline-extract-restaurant-inspection.py" --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"
```