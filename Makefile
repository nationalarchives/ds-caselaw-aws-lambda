build:
	@mkdir -p dist
	@rm dist/lambda.zip & 2>&1
	@samlocal build --use-container -m requirements/base.txt
	@cd .aws-sam/build/TNACaselawIngesterFunction && zip -r ../../../dist/lambda.zip .
	@zip -g dist/lambda.zip ds-caselaw-ingester/lambda_function.py
	@echo 'Built dist/lambda.zip'

ifeq (setup,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "setup"
  RUN_ARG := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARG):;@:)
endif

setup:
	make build
	sh scripts/setup-localstack.sh $(RUN_ARG)

update:
	make build
	@sh scripts/update-lambda.sh

send-message-v1:
	@awslocal sns publish --topic-arn arn:aws:sns:us-east-1:000000000000:judgments --message file://aws_examples/sns/parsed-judgment.json

send-message-v2:
	@awslocal sns publish --topic-arn arn:aws:sns:us-east-1:000000000000:judgments --message file://aws_examples/sns/parsed-judgment-v2.json

send-message-s3:
	@awslocal s3 cp aws_examples/s3/te-editorial-out-int/test3.tar.gz s3://inbound-bucket/QX/e31b117f-ff09-49b6-a697-7952c7a67384/QX.tar.gz

send-message-v2-press-summary:
	@awslocal sns publish --topic-arn arn:aws:sns:us-east-1:000000000000:judgments --message file://aws_examples/sns/press-summary.json


delete-document:
	@curl --anyauth --user admin:admin -X DELETE -i http://localhost:8000/v1/documents\?database\=Judgments\&uri\=/ewca/civ/2022/111.xml
