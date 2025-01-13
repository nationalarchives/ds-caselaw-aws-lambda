source .env

awslocal iam create-role \
  --role-name lambda-role \
  --assume-role-policy-document file://aws_examples/example_trust_policy.json

awslocal lambda create-function \
  --function-name te-lambda \
  --zip-file fileb://dist/lambda.zip \
  --handler ds-caselaw-ingester/lambda_function.handler \
  --runtime python3.11 \
  --environment "Variables={MARKLOGIC_HOST=$MARKLOGIC_HOST,MARKLOGIC_USER=$MARKLOGIC_USER,MARKLOGIC_PASSWORD=$MARKLOGIC_PASSWORD,AWS_BUCKET_NAME=$AWS_BUCKET_NAME,AWS_SECRET_KEY=$AWS_SECRET_KEY,AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID,AWS_ENDPOINT_URL=$AWS_ENDPOINT_URL,SQS_QUEUE_URL=$SQS_QUEUE_URL,ROLLBAR_TOKEN=$ROLLBAR_TOKEN,ROLLBAR_ENV=$ROLLBAR_ENV,NOTIFY_API_KEY=$NOTIFY_API_KEY,NOTIFY_EDITORIAL_ADDRESS=$NOTIFY_EDITORIAL_ADDRESS,NOTIFY_NEW_JUDGMENT_TEMPLATE_ID=$NOTIFY_NEW_JUDGMENT_TEMPLATE_ID,EDITORIAL_UI_BASE_URL=$EDITORIAL_UI_BASE_URL,PUBLIC_ASSET_BUCKET=$PUBLIC_ASSET_BUCKET,LAMBDA_RUNTIME_ENVIRONMENT_TIMEOUT=500}" \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --timeout 500

awslocal sns create-topic \
  --name judgments \
  --attributes consignment-reference=string,s3-folder-url=string,consignment-type=string,number-of-retries=number

awslocal sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:judgments \
  --protocol lambda \
  --notification-endpoint arn:aws:lambda:us-east-1:000000000000:function:te-lambda

awslocal s3api create-bucket \
  --bucket te-editorial-out-int

awslocal s3api create-bucket \
  --bucket judgments-original-versions

awslocal s3api create-bucket \
  --bucket public-asset-bucket

awslocal s3api create-bucket \
  --bucket private-asset-bucket


awslocal sns create-topic \
  --name inbound-sns \
  --attributes consignment-reference=string,s3-folder-url=string,consignment-type=string,number-of-retries=number

awslocal s3api create-bucket \
  --bucket inbound-bucket

awslocal s3api put-bucket-notification-configuration \
  --bucket inbound-bucket \
  --notification-configuration file://scripts/inbound-s3-sns.json

awslocal sns subscribe --protocol lambda \
--region us-east-1 \
--topic-arn arn:aws:sns:us-east-1:000000000000:inbound-sns \
--notification-endpoint arn:aws:lambda:us-east-1:000000000000:function:te-lambda


if [ -n "$1" ]; then
  awslocal s3 cp $1 s3://te-editorial-out-int
else
  awslocal s3 cp aws_examples/s3/te-editorial-out-int/TDR-2022-DNWR.tar.gz s3://te-editorial-out-int
fi

awslocal s3api create-bucket --bucket staging-tre-court-document-pack-out
awslocal s3 cp aws_examples/s3/te-editorial-out-int/TDR-2022-DNWR.tar.gz s3://staging-tre-court-document-pack-out/QX/e31b117f-ff09-49b6-a697-7952c7a67384/QX.tar.gz
awslocal s3 cp aws_examples/s3/te-editorial-out-int/press-summary.tar.gz s3://staging-tre-court-document-pack-out/QX/press-summary/QX.tar.gz
awslocal sqs create-queue --queue-name retry-queue
