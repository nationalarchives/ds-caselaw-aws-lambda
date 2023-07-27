# The National Archives: Find Case Law

This repository is part of the [Find Case Law](https://caselaw.nationalarchives.gov.uk/) project at [The National Archives](https://www.nationalarchives.gov.uk/). For more information on the project, check [the documentation](https://github.com/nationalarchives/ds-find-caselaw-docs).

# Case Law Ingester

This is the repository for the lambda function used to parse Transformation Engine judgments and insert them to Marklogic

## Development

We're using [localstack](https://github.com/localstack/localstack), along with the awslocal-cli to enable local development of the lambda function.

### Requirements

An installation of `make` is required to use the bundled Makefile for local development. Most operating systems come with this preinstalled, including Ubuntu Linux and MacOS. On Windows, Make can be installed via the Chocolatey package manager, or using the Windows Subsystem for Linux (WSL).

You will also need both `awscli` and `awslocal-cli` installed. `awslocal-cli` is a `Localstack`-specific wrapper around `awscli`.

Install both from the requirements file using:

```bash
python3 -m pip install -r requirements/local.txt
```

### Setup Localstack

First, copy `.env.example` to `.env` and fill in the missing variables. If you are using Localstack via Docker, leave `MARKLOGIC_HOST` as `host.docker.internal`.

Then, start Localstack using:

```bash
docker compose up -d
```

This will start Localstack in detached mode; logs are accessible via Docker Desktop.

Once the docker container is running, use the following make command to build a distribution of the lambda function, and setup the localstack AWS services

```bash
make setup
```

This will create a folder, `dist`, on your local machine that contains a zip file called `lambda.zip` - this is our compiled lambda. You can also upload this directly to the AWS console.

### Sending a message

To send the example message bundled, use the `send-message-v2` make target:

```bash
make send-message-v2
```

This will publish a message to the SNS topic, triggering the `handle` function in our lambda.

(`send-message-v1` exists, and sends a v1 message.)

### Viewing Output

The lambda output will be logged in the Localstack logs. Look for the lines following:

```
localstack.services.awslambda.lambda_executors: Lambda arn:aws:lambda:us-east-1:000000000000:function:te-lambda result / log output:
```

The logs will show the response from the lambda directly below this line. Any values sent to stdout (e.g. `print` statements), will be output beneath.

### Unit tests

To run the tests

* [First time] create a virtualenv (`virtualenv venv -p \`which python\`` )
* Activate it with `. venv/bin/activate`
* `scripts/test`
* When you're done, you might want to `deactivate`

Note that you might get a spurious errors about django config and environment variables if you're running in the wrong environment.

### Updating the lambda

If you make a change to the code and need to update the lambda function, use the `update` make command:

```bash
make update
```

And then send a message:

```bash
make send-message-v2
```

## Local testing

To test a tarfile locally:

1. Add your test tarfile to `aws_examples/s3/te-editorial-out-int`.
2. Edit `aws_examples/sns/parsed-judgment.json` to contain your tarfile name in `s3-folder-url` and consignment reference
   in `consignment-reference`.
3. Run `make setup aws_examples/s3/te-editorial-out-int/<your tarfile>`, for example `make setup aws_examples/s3/te-editorial-out-int/XYZ-123.tar.gz`.
   If you run `make setup` without an argument, the original test tarfile `TDR-2022-DNWR.tar.gz` will be used
4. Run `make send-message-v2` to ingest your tarfile.

## Deployment

Every change to the `main` branch is automatically deployed to the staging environment via GitHub actions.

Only releases are deployed to production. To trigger a deploy, [create a new release](https://github.com/nationalarchives/ds-caselaw-ingester/releases/new) named and tagged `vX.Y.Z` following semantic versioning. Autogenerate release notes, and publish; the release will then be tagged `latest` automatically and deployed to production.
