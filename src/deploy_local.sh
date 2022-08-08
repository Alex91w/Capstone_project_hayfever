cd src
sh build_source_code.sh

cd ../infrastructure
terraform init
terraform apply -auto-approve