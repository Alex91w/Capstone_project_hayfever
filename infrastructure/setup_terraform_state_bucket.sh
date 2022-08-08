TERRAFORM_S3_BUCKET_NAME=alexwbucket1234
if aws s3 ls "s3://$TERRAFORM_S3_BUCKET_NAME" 2>&1 | grep -q 'An error occurred'
then
    aws s3api create-bucket --bucket $TERRAFORM_S3_BUCKET_NAME --region us-west-2 --create-bucket-configuration LocationConstraint=eu-central-1
else
    echo "terraform bucket already exists"
fi