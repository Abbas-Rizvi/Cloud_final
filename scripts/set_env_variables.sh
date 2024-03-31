export PROJECT=$(gcloud config list project --format "value(core.project)")
export BUCKET="gs://highway-dataset"

echo "USING:"
echo "PROJECT: $PROJECT"
echo "BUCKET: $BUCKET"
