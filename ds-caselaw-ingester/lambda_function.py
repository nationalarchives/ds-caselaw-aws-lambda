import json
import logging
import os
import re
import tarfile
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Tuple, Union
from xml.sax.saxutils import escape

import boto3
import rollbar
import urllib3
from boto3.session import Session
from botocore.exceptions import NoCredentialsError
from caselawclient.Client import (
    MarklogicCommunicationError,
    MarklogicResourceNotFoundError,
    api_client,
)
from notifications_python_client.notifications import NotificationsAPIClient

rollbar.init(os.getenv("ROLLBAR_TOKEN"), environment=os.getenv("ROLLBAR_ENV"))


class Message(object):
    @classmethod
    def from_event(cls, event):
        decoder = json.decoder.JSONDecoder()
        message = decoder.decode(event["Records"][0]["Sns"]["Message"])
        return cls.from_message(message)

    @classmethod
    def from_message(cls, message):
        if "parameters" in message.keys():
            return V2Message(message)
        else:
            return V1Message(message)

    def __init__(self, message):
        self.message = message


class V1Message(Message):
    def is_v1(self):
        return True

    def get_consignment_reference(self):
        try:
            result = self.message.get("consignment-reference", "")

            if not result:
                tarfile_location = self.message["s3-folder-url"]
                tarfile_name = re.findall("([^/]+$)", tarfile_location)[0]
                result = tarfile_name.partition(".tar.gz")[0]

            return result
        except KeyError:
            raise InvalidMessageException(
                "Malformed v1 message, please supply a consignment-reference or s3-folder-url"
            )

    def save_s3_response(self, sqs_client, s3_client):
        http = urllib3.PoolManager()

        try:
            s3_response = http.request("GET", self.message["s3-folder-url"])
            tar_gz_contents = s3_response.data
            if s3_response.status >= 400:
                raise S3HTTPError(tar_gz_contents[:250])
        except Exception:
            # Send retry message to sqs if the GET fails
            send_retry_message(self.message, sqs_client)
            raise
        # Store it in the /tmp directory

        reference = self.get_consignment_reference()
        filename = os.path.join("/tmp", f"{reference}.tar.gz")

        with open(filename, "wb") as out:
            out.write(tar_gz_contents)
            out.close()

        return filename


class V2Message(Message):
    def is_v1(self):
        return False

    def get_consignment_reference(self):
        result = self.message.get("parameters", {}).get("reference")
        if result:
            return result

        raise InvalidMessageException(
            "Malformed v2 message, please supply a consignment-reference or s3-folder-url"
        )

    def save_s3_response(self, sqs_client, s3_client):
        s3_bucket = self.message.get("parameters", {}).get("s3Bucket")
        s3_key = self.message.get("parameters", {}).get("s3Key")
        reference = self.get_consignment_reference()
        filename = os.path.join("/tmp", f"{reference}.tar.gz")
        s3_client.download_file(s3_bucket, s3_key, filename)
        if not os.path.exists(filename):
            raise RuntimeError(f"File {filename} not created")
        print(f"tar.gz saved locally as {filename}")
        return filename


class ReportableException(Exception):
    def __init__(self, *args, **kwargs):
        rollbar.report_message("Something happened!", "warning", str(self))
        super().__init__(*args, **kwargs)


class S3HTTPError(ReportableException):
    pass


class FileNotFoundException(ReportableException):
    pass


class DocxFilenameNotFoundException(ReportableException):
    pass


class MaximumRetriesExceededException(ReportableException):
    pass


class InvalidXMLException(ReportableException):
    pass


class InvalidMessageException(ReportableException):
    pass


class DocumentInsertionError(ReportableException):
    pass


def extract_xml_file(tar: tarfile, xml_file_name: str):
    xml_file = None
    if xml_file_name:
        for member in tar.getmembers():
            if xml_file_name in member.name:
                xml_file = tar.extractfile(member)

    return xml_file


def extract_metadata(tar: tarfile, consignment_reference: str):
    te_metadata_file = None
    decoder = json.decoder.JSONDecoder()
    for member in tar.getmembers():
        if "metadata.json" in member.name:
            te_metadata_file = tar.extractfile(member)

    if te_metadata_file is None:
        raise FileNotFoundException(
            f"Metadata file not found. Consignment Ref: {consignment_reference}"
        )
    return decoder.decode(te_metadata_file.read().decode("utf-8"))


def extract_uri(metadata: dict, consignment_reference: str) -> str:
    uri = metadata["parameters"]["PARSER"].get("uri", "")

    if uri:
        uri = uri.replace("https://caselaw.nationalarchives.gov.uk/id/", "")

    if not uri:
        uri = f"failures/{consignment_reference}"

    return uri


def is_v1(message):
    return Message.from_message(message).is_v1()


# called by tests
def get_consignment_reference(message):
    return Message.from_message(message).get_consignment_reference()


def extract_docx_filename(metadata: dict, consignment_reference: str) -> str:
    try:
        return metadata["parameters"]["TRE"]["payload"]["filename"]
    except KeyError:
        raise DocxFilenameNotFoundException(
            f"No .docx filename was found in metadata. Consignment Ref: {consignment_reference}"
        )


def extract_lambda_versions(versions: List[Dict[str, str]]) -> List[Tuple[str, str]]:
    version_tuples = []
    for d in versions:
        version_tuples += list(d.items())

    return version_tuples


def store_metadata(uri: str, metadata: dict) -> None:
    tdr_metadata = metadata["parameters"]["TDR"]

    # Store source information
    api_client.set_property(
        uri, name="source-organisation", value=tdr_metadata["Source-Organization"]
    )
    api_client.set_property(uri, name="source-name", value=tdr_metadata["Contact-Name"])
    api_client.set_property(
        uri, name="source-email", value=tdr_metadata["Contact-Email"]
    )
    # Store TDR data
    api_client.set_property(
        uri,
        name="transfer-consignment-reference",
        value=tdr_metadata["Internal-Sender-Identifier"],
    )
    api_client.set_property(
        uri,
        name="transfer-received-at",
        value=tdr_metadata["Consignment-Completed-Datetime"],
    )


def store_file(file, folder, filename, s3_client: Session.client):
    pathname = f"{folder}/{filename}"
    try:
        s3_client.upload_fileobj(file, os.getenv("AWS_BUCKET_NAME"), pathname)
        print(f"Upload Successful {pathname}")
    except FileNotFoundError:
        print(f"The file {pathname} was not found")
    except NoCredentialsError:
        print("Credentials not available")


def send_new_judgment_notification(uri: str, metadata: dict) -> None:
    if os.getenv("ROLLBAR_ENV") != "prod":
        return
    tdr_metadata = metadata["parameters"]["TDR"]
    notifications_client = NotificationsAPIClient(os.getenv("NOTIFY_API_KEY"))
    response = notifications_client.send_email_notification(
        email_address=os.getenv("NOTIFY_EDITORIAL_ADDRESS"),
        template_id=os.getenv("NOTIFY_NEW_JUDGMENT_TEMPLATE_ID"),
        personalisation={
            "url": f'{os.getenv("EDITORIAL_UI_BASE_URL")}detail?judgment_uri={uri}',
            "consignment": tdr_metadata["Internal-Sender-Identifier"],
            "submitter": f'{tdr_metadata["Contact-Name"]}, {tdr_metadata["Source-Organization"]}'
            f' <{tdr_metadata["Contact-Email"]}>',
            "submitted_at": tdr_metadata["Consignment-Completed-Datetime"],
        },
    )
    print(
        f'Sent notification to {os.getenv("NOTIFY_EDITORIAL_ADDRESS")} (Message ID: {response["id"]})'
    )


def copy_file(tarfile, input_filename, output_filename, uri, s3_client: Session.client):
    try:
        file = tarfile.extractfile(input_filename)
        store_file(file, uri, output_filename, s3_client)
    except KeyError:
        raise FileNotFoundException(f"File was not found: {input_filename}")


def send_retry_message(
    original_message: Dict[str, Union[str, int]], sqs_client: Session.client
) -> None:
    if not is_v1(original_message):
        raise MaximumRetriesExceededException("v2 messages cannot yet be retried")

    retry_number = int(original_message["number-of-retries"]) + 1
    if retry_number <= int(os.getenv("MAX_RETRIES", "5")):
        retry_message = {
            "consignment-reference": original_message["consignment-reference"],
            "s3-folder-url": "",
            "consignment-type": original_message["consignment-type"],
            "number-of-retries": retry_number,
        }
        sqs_client.send_message(
            QueueUrl=os.getenv("SQS_QUEUE_URL"),
            MessageBody=json.dumps(retry_message),
        )
    else:
        raise MaximumRetriesExceededException(
            f'Maximum number of retries reached for {original_message["consignment-reference"]}'
        )


def create_parser_log_xml(tar):
    parser_log_value = "<error>parser.log not found</error>"
    for member in tar.getmembers():
        if "parser.log" in member.name:
            parser_log = tar.extractfile(member)
            parser_log_contents = escape(parser_log.read().decode("utf-8"))
            parser_log_value = f"<error>{parser_log_contents}</error>"
    return parser_log_value


def parse_xml(xml) -> ET.Element:
    ET.register_namespace("", "http://docs.oasis-open.org/legaldocml/ns/akn/3.0")
    ET.register_namespace("uk", "https://caselaw.nationalarchives.gov.uk/akn")
    return ET.XML(xml)


def update_judgment_xml(uri, xml) -> bool:
    try:
        api_client.get_judgment_xml(uri, show_unpublished=True)
        api_client.save_judgment_xml(uri, xml)
        return True
    except (MarklogicResourceNotFoundError, MarklogicCommunicationError):
        return False


def insert_document_xml(uri, xml) -> bool:
    try:
        api_client.insert_document_xml(uri, xml)
        return True
    except MarklogicCommunicationError:
        return False


def select_uri(source_uri):
    """If the judgment exists, suggest a URL that is not going to be collided with,
    even if there are multiple requests a second or the ingester is restarted."""
    if not api_client.judgment_exists(source_uri):
        return source_uri
    else:
        now = datetime.now().strftime("%Y%m%dT%H%M%S")
        return f"collisions/{now}/{uuid.uuid4()}"


def get_best_xml(uri, tar, xml_file_name, consignment_reference):
    xml_file = extract_xml_file(tar, xml_file_name)
    if xml_file:
        contents = xml_file.read()
        try:
            return parse_xml(contents)
        except ET.ParseError:
            logging.warning(
                f"Invalid XML file for uri: {uri}, consignment reference: {consignment_reference}."
                f" Falling back to parser.log contents."
            )
            contents = create_parser_log_xml(tar)
            return parse_xml(contents)
    else:
        logging.warning(
            f"No XML file found in tarfile for uri: {uri}, filename: {xml_file_name},"
            f"consignment reference: {consignment_reference}."
            f" Falling back to parser.log contents."
        )
        contents = create_parser_log_xml(tar)
        return parse_xml(contents)


def unpublish_updated_judgment(uri):
    api_client.set_published(uri, False)


@rollbar.lambda_function
def handler(event, context):
    message = Message.from_event(event)

    consignment_reference = message.get_consignment_reference()
    print(f"Ingester Start: Consignment reference {consignment_reference}")
    print(f"Received Message: {message.message}")
    print(f"v1: {message.is_v1()}")

    if (
        os.getenv("AWS_ACCESS_KEY_ID")
        and os.getenv("AWS_SECRET_KEY")
        and os.getenv("AWS_ENDPOINT_URL")
    ):
        session = boto3.session.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        )
        sqs_client = session.client("sqs", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
        s3_client = session.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    else:
        session = boto3.session.Session()
        sqs_client = session.client("sqs")
        s3_client = session.client("s3")

    # Retrieve tar file from S3
    filename = message.save_s3_response(sqs_client, s3_client)

    tar = tarfile.open(filename, mode="r")
    metadata = extract_metadata(tar, consignment_reference)

    # Extract and parse the judgment XML
    xml_file_name = metadata["parameters"]["TRE"]["payload"]["xml"]
    source_uri = extract_uri(metadata, consignment_reference)
    target_uri = select_uri(source_uri)
    print(f"Ingesting document {source_uri} at {target_uri}")
    xml = get_best_xml(target_uri, tar, xml_file_name, consignment_reference)

    inserted = insert_document_xml(target_uri, xml)

    if inserted:
        # Notify editors that a new document is ready
        send_new_judgment_notification(target_uri, metadata)
        print(f"Inserted judgment xml for {source_uri} at {target_uri}")
    else:
        raise DocumentInsertionError(
            f"Judgment {source_uri} failed to insert into Marklogic at {target_uri}."
            f"Consignment Ref: {consignment_reference}"
        )

    # Store metadata
    store_metadata(target_uri, metadata)

    # Store docx and rename
    docx_filename = extract_docx_filename(metadata, consignment_reference)
    copy_file(
        tar,
        f"{consignment_reference}/{docx_filename}",
        f'{target_uri.replace("/", "_")}.docx',
        target_uri,
        s3_client,
    )

    # Store parser log
    try:
        copy_file(
            tar,
            f"{consignment_reference}/parser.log",
            "parser.log",
            target_uri,
            s3_client,
        )
    except FileNotFoundException:
        pass

    # Store images
    image_list = metadata["parameters"]["TRE"]["payload"]["images"]
    if image_list:
        for image_filename in image_list:
            copy_file(
                tar,
                f"{consignment_reference}/{image_filename}",
                image_filename,
                target_uri,
                s3_client,
            )

    # Copy original tarfile
    store_file(
        open(filename, mode="rb"), target_uri, os.path.basename(filename), s3_client
    )

    tar.close()

    print("Ingestion complete")

    return message.message
