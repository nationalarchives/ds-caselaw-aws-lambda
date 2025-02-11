import json
import logging
import os
import tarfile
import xml.etree.ElementTree as ET
from contextlib import suppress
from typing import Any, TypedDict
from urllib.parse import unquote_plus
from uuid import uuid4
from xml.sax.saxutils import escape

import boto3
import boto3.s3
import rollbar
from botocore.exceptions import NoCredentialsError
from caselawclient.Client import (
    DEFAULT_USER_AGENT,
    MarklogicApiClient,
    MarklogicResourceNotFoundError,
)
from caselawclient.client_helpers import VersionAnnotation, VersionType
from caselawclient.models.documents import DocumentURIString
from caselawclient.models.identifiers.neutral_citation import NeutralCitationNumber
from caselawclient.models.identifiers.press_summary_ncn import PressSummaryRelatedNCNIdentifier
from caselawclient.models.press_summaries import PressSummary
from caselawclient.models.utilities.aws import S3PrefixString
from dotenv import load_dotenv
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import CopySourceTypeDef
from notifications_python_client.notifications import NotificationsAPIClient

logger = logging.getLogger("ingester")
logger.setLevel(logging.DEBUG)

load_dotenv()

rollbar.init(os.getenv("ROLLBAR_TOKEN"), environment=os.getenv("ROLLBAR_ENV"))

MARKLOGIC_HOST: str = os.environ["MARKLOGIC_HOST"]
MARKLOGIC_USER: str = os.environ["MARKLOGIC_USER"]
MARKLOGIC_PASSWORD: str = os.environ["MARKLOGIC_PASSWORD"]
MARKLOGIC_USE_HTTPS: bool = bool(os.getenv("MARKLOGIC_USE_HTTPS", default=False))

AWS_BUCKET_NAME: str = os.environ["AWS_BUCKET_NAME"]

api_client = MarklogicApiClient(
    host=MARKLOGIC_HOST,
    username=MARKLOGIC_USER,
    password=MARKLOGIC_PASSWORD,
    use_https=MARKLOGIC_USE_HTTPS,
    user_agent=f"ds-caselaw-ingester/unknown {DEFAULT_USER_AGENT}",
)


class TREMetadataDict(TypedDict):
    parameters: dict[str, Any]


class SubmitterInformationDict(TypedDict):
    name: str
    email: str


class VersionPayloadDict(TypedDict, total=False):
    tre_raw_metadata: TREMetadataDict
    tdr_reference: str
    submitter: SubmitterInformationDict


class Metadata:
    def __init__(self, metadata):
        self.metadata: TREMetadataDict = metadata
        self.parameters = metadata.get("parameters", {})

    @property
    def is_tdr(self) -> bool:
        return "TDR" in self.parameters

    @property
    def force_publish(self) -> bool:
        return self.parameters.get("INGESTER_OPTIONS", {}).get("auto_publish", False)


class Message:
    @classmethod
    def from_message(cls, message: dict):
        if message.get("Records", [{}])[0].get("eventSource") == "aws:s3":
            return S3Message(message["Records"][0])
        elif "parameters" in message:
            return V2Message(message)
        else:
            raise InvalidMessageException(f"Did not recognise message type. {message}")

    def __init__(self, message):
        self.message = message

    @property
    def originator(self):
        # potential values are:
        # Original message from TDR: 'TDR'
        # Reparse message from FCL: 'FCL'
        # Bulk parse message from FCL: 'FCL S3'
        raise NotImplementedError("Bare Message objects do not have an originator")

    def update_consignment_reference(self, new_ref):
        """In most cases we trust we already have the correct consignment reference"""
        return

    def get_consignment_reference(*args, **kwargs):
        raise NotImplementedError("defer to subclasses")


class V2Message(Message):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def originator(self) -> str:
        return self.message.get("parameters", {}).get("originator")

    def get_consignment_reference(self):
        """A strange quirk: the consignment reference we recieve from the V2 message is
        of the form TDR-2000-123, but the consignment reference inside the document is
        of the form TRE-TDR-2000-123. The folder in the .tar.gz file is TDR-2000-123,
        it reflects the V2 message format, not the format found within the tar.gz"""
        result = self.message.get("parameters", {}).get("reference")
        if result:
            return result

        raise InvalidMessageException("Malformed v2 message, please supply a reference")

    def save_s3_response(self, sqs_client, s3_client) -> str:
        s3_bucket = self.message.get("parameters", {}).get("s3Bucket")
        s3_key = self.message.get("parameters", {}).get("s3Key")
        reference = self.get_consignment_reference()
        local_tar_filename = os.path.join("/tmp", f"{reference}.tar.gz")
        s3_client.download_file(s3_bucket, s3_key, local_tar_filename)
        if not os.path.exists(local_tar_filename):
            raise RuntimeError(f"File {local_tar_filename} not created")
        print(f"tar.gz saved locally as {local_tar_filename}")
        return local_tar_filename


class S3Message(V2Message):
    """An SNS message generated directly by adding a file to an S3 bucket"""

    def __init__(self, *args, **kwargs):
        self._consignment = None
        super().__init__(*args, **kwargs)

    @property
    def originator(self):
        return "FCL S3"

    def get_consignment_reference(self):
        """We use the filename as a first draft of the consignment reference,
        but later update it with the value from the tar gz. Note that this
        behaviour is totally inconsistent with the behaviour of the V2 message
        where the consignment reference in the metadata is ignored."""
        if self._consignment:
            return self._consignment
        return self.message["s3"]["object"]["key"].split("/")[-1].partition(".")[0]

    def update_consignment_reference(self, new_ref):
        self._consignment = new_ref

    def save_s3_response(self, sqs_client, s3_client):
        s3_key = unquote_plus(self.message["s3"]["object"]["key"])
        s3_bucket = self.message["s3"]["bucket"]["name"]
        reference = self.get_consignment_reference()
        local_tar_filename = os.path.join("/tmp", f"{reference}.tar.gz")
        s3_client.download_file(s3_bucket, s3_key, local_tar_filename)
        if not os.path.exists(local_tar_filename):
            raise RuntimeError(f"File {local_tar_filename} not created")
        print(f"tar.gz saved locally as {local_tar_filename}")
        return local_tar_filename


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


class ErrorLogWouldOverwritePublishedDocument(ReportableException):
    pass


def modify_filename(original: str, addition: str) -> str:
    "Add an addition after the filename, so TRE-2024-A.tar.gz becomes TRE-2024-A_nodocx.tar.gz"
    path, basename = os.path.split(original)
    # dot will be an empty string if there is no dot in the filename.
    # prefix will be everything upto and not including the first dot.
    prefix, dot, suffix = basename.partition(".")
    new_basename = f"{prefix}{addition}{dot}{suffix}"
    return os.path.join(path, new_basename)


def all_messages(event) -> list[Message]:
    """All the messages in the SNS event, as Message subclasses"""
    decoder = json.decoder.JSONDecoder()
    messages_as_decoded_json = [decoder.decode(record["Sns"]["Message"]) for record in event["Records"]]
    return [Message.from_message(message) for message in messages_as_decoded_json]


def extract_xml_file(tar: tarfile.TarFile, xml_file_name: str):
    xml_file = None
    if xml_file_name:
        for member in tar.getmembers():
            if xml_file_name in member.name:
                xml_file = tar.extractfile(member)
    return xml_file


def extract_metadata(tar: tarfile.TarFile, consignment_reference: str) -> TREMetadataDict:
    te_metadata_file = None
    decoder = json.decoder.JSONDecoder()
    for member in tar.getmembers():
        if "-metadata.json" in member.name:
            te_metadata_file = tar.extractfile(member)

    if te_metadata_file is None:
        raise FileNotFoundException(f"Metadata file not found. Consignment Ref: {consignment_reference}")
    return decoder.decode(te_metadata_file.read().decode("utf-8"))


# called by tests
def get_consignment_reference(message):
    return Message.from_message(message).get_consignment_reference()


def extract_docx_filename(metadata: TREMetadataDict, consignment_reference: str) -> str:
    try:
        return metadata["parameters"]["TRE"]["payload"]["filename"]
    except KeyError as err:
        raise DocxFilenameNotFoundException(
            f"No .docx filename was found in metadata. Consignment Ref: {consignment_reference}, metadata: {metadata}",
        ) from err


def extract_lambda_versions(versions: list[dict[str, str]]) -> list[tuple[str, str]]:
    version_tuples = []
    for d in versions:
        version_tuples += list(d.items())

    return version_tuples


def store_file(file, destination_folder: S3PrefixString, destination_filename: str, s3_client: S3Client):
    """Given a file, store it in the specified location in S3."""
    pathname: str = destination_folder + destination_filename
    try:
        s3_client.upload_fileobj(file, AWS_BUCKET_NAME, pathname)
        print(f"Upload Successful {pathname}")
    except FileNotFoundError:
        print(f"The file {pathname} was not found")
    except NoCredentialsError:
        print("Credentials not available")


def personalise_email(uri: str, metadata: TREMetadataDict) -> dict:
    """Doesn't contain 'doctype', re-add for new judgment notification"""
    try:
        tdr_metadata = metadata["parameters"]["TDR"]
    except KeyError:
        tdr_metadata = {}
    return {
        "url": f"{os.getenv('EDITORIAL_UI_BASE_URL')}detail?judgment_uri={uri}",
        "consignment": tdr_metadata.get("Internal-Sender-Identifier", "unknown"),
        "submitter": f"{tdr_metadata.get('Contact-Name', 'unknown')}, "
        f"{tdr_metadata.get('Source-Organization', 'unknown')}"
        f" <{tdr_metadata.get('Contact-Email', 'unknown')}>",
        "submitted_at": tdr_metadata.get("Consignment-Completed-Datetime", "unknown"),
    }


def copy_file(
    tarfile: tarfile.TarFile,
    input_filename: str,
    output_filename: str,
    output_location: S3PrefixString,
    s3_client: S3Client,
) -> None:
    """Copy the specified file from the input tar to the destination location."""
    try:
        file = tarfile.extractfile(input_filename)
        store_file(
            file=file,
            destination_folder=output_location,
            destination_filename=output_filename,
            s3_client=s3_client,
        )
    except KeyError as err:
        raise FileNotFoundException(f"File was not found: {input_filename}, files were {tarfile.getnames()} ") from err


def create_parser_log_xml(tar: tarfile.TarFile) -> str:
    parser_log_value = "<error>parser.log not found</error>"
    for member in tar.getmembers():
        if "parser.log" in member.name:
            parser_log = tar.extractfile(member)
            if parser_log is not None:
                parser_log_contents = escape(parser_log.read().decode("utf-8"))
            else:
                parser_log_contents = "Unable to read parser log file!"
            parser_log_value = f"<error>{parser_log_contents}</error>"
    return parser_log_value


def update_published_documents(uri, s3_client: S3Client) -> None:
    public_bucket = os.environ["PUBLIC_ASSET_BUCKET"]
    private_bucket = os.environ["AWS_BUCKET_NAME"]

    response = s3_client.list_objects(Bucket=private_bucket, Prefix=uri)

    for result in response.get("Contents", []):
        key = result["Key"]

        if "parser.log" not in key and not str(key).endswith(".tar.gz"):
            source: CopySourceTypeDef = {"Bucket": private_bucket, "Key": key}
            extra_args: dict[str, Any] = {}
            s3_client.copy(source, public_bucket, key, extra_args)


def parse_xml(xml: str) -> ET.Element:
    ET.register_namespace("", "http://docs.oasis-open.org/legaldocml/ns/akn/3.0")
    ET.register_namespace("uk", "https://caselaw.nationalarchives.gov.uk/akn")
    return ET.XML(xml)


def _build_version_annotation_payload_from_metadata(metadata: TREMetadataDict) -> VersionPayloadDict:
    """Turns metadata from TRE into a structured annotation payload."""
    payload: VersionPayloadDict = {
        "tre_raw_metadata": metadata,
    }

    if "TDR" in metadata["parameters"]:
        payload["tdr_reference"] = metadata["parameters"]["TDR"]["Internal-Sender-Identifier"]
        payload["submitter"] = {
            "name": metadata["parameters"]["TDR"]["Contact-Name"],
            "email": metadata["parameters"]["TDR"]["Contact-Email"],
        }

    return payload


def get_best_xml(uri, tar, xml_file_name: str, consignment_reference: str) -> ET.Element:
    xml_file = extract_xml_file(tar, xml_file_name)
    if xml_file:
        contents = xml_file.read()
        try:
            return parse_xml(contents)
        except ET.ParseError:
            print(
                f"Invalid XML file for uri: {uri}, consignment reference: {consignment_reference}."
                f" Falling back to parser.log contents.",
            )
            contents = create_parser_log_xml(tar)
            return parse_xml(contents)
    else:
        print(
            f"No XML file found in tarfile for uri: {uri}, filename: {xml_file_name},"
            f"consignment reference: {consignment_reference}."
            f" Falling back to parser.log contents.",
        )
        contents = create_parser_log_xml(tar)
        return parse_xml(contents)


def aws_clients():
    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_KEY") and os.getenv("AWS_ENDPOINT_URL"):
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
    return sqs_client, s3_client


class Ingest:
    @classmethod
    def from_message_dict(cls, message_dict: dict) -> "Ingest":
        return Ingest(Message.from_message(message_dict))

    def __init__(self, message: Message) -> None:
        self.message = message
        self.consignment_reference: str = self.message.get_consignment_reference()
        print(f"Ingester Start: Consignment reference {self.consignment_reference}")
        print(f"Received Message: {self.message.message}")
        self.local_tar_filename = self.save_tar_file_in_s3()
        self.uri = DocumentURIString("d-" + str(uuid4()))
        with tarfile.open(self.local_tar_filename, mode="r") as tar:
            self.metadata = extract_metadata(tar, self.consignment_reference)
            self.message.update_consignment_reference(self.metadata["parameters"]["TRE"]["reference"])
            self.xml_file_name = self.metadata["parameters"]["TRE"]["payload"]["xml"]
            self.xml = get_best_xml(self.uri, tar, self.xml_file_name, self.consignment_reference)
        print(f"Ingesting document {self.uri}")

    def save_tar_file_in_s3(self):
        """This should be mocked out for testing -- get the tar file from S3 and
        save locally, returning the filename it was saved at"""
        sqs_client, s3_client = aws_clients()
        return self.message.save_s3_response(sqs_client, s3_client)

    def update_document_xml(self) -> bool:
        if self.metadata_object.is_tdr:
            message = "Updated document submitted by TDR user"
        else:
            message = "Updated document uploaded by Find Case Law"
        try:
            annotation = VersionAnnotation(
                VersionType.SUBMISSION,
                automated=self.metadata_object.force_publish,
                message=message,
                payload=dict(
                    _build_version_annotation_payload_from_metadata(self.metadata),
                ),  # We cast this to a dict here because VersionAnnotation doesn't yet have a TypedDict as its payload argument.
            )

            api_client.get_judgment_xml(self.uri, show_unpublished=True)
            api_client.update_document_xml(self.uri, self.xml, annotation)
            return True
        except MarklogicResourceNotFoundError:
            return False

    def insert_document_xml(self) -> bool:
        if self.metadata_object.is_tdr:
            message = "New document submitted by TDR user"
        else:
            message = "New document uploaded by Find Case Law"
        annotation = VersionAnnotation(
            VersionType.SUBMISSION,
            automated=self.metadata_object.force_publish,
            message=message,
            payload=dict(
                _build_version_annotation_payload_from_metadata(self.metadata),
            ),  # We cast this to a dict here because VersionAnnotation doesn't yet have a TypedDict as its payload argument.
        )
        api_client.insert_document_xml(self.uri, self.xml, annotation)
        return True

    def set_document_identifiers(self) -> None:
        doc = api_client.get_document_by_uri(DocumentURIString(self.uri))
        if doc.identifiers:
            msg = f"Ingesting, but identifiers already present for {self.uri}!"
            logger.warning(msg)

        ncn = doc.neutral_citation
        identifier_class = PressSummaryRelatedNCNIdentifier if isinstance(doc, PressSummary) else NeutralCitationNumber

        if ncn:
            doc.identifiers.add(identifier_class(ncn))
            doc.save_identifiers()
            logger.info(f"Ingested document had identifier {identifier_class.__name__} {ncn}")
        else:
            logger.info("Ingested document had NCN (NOT FOUND)")

    def send_updated_judgment_notification(self) -> None:
        personalisation = personalise_email(self.uri, self.metadata)
        if os.getenv("ROLLBAR_ENV") != "prod":
            print(f"Would send a notification but we're not in production.\n{personalisation}")
            return

        notifications_client = NotificationsAPIClient(os.environ["NOTIFY_API_KEY"])
        response = notifications_client.send_email_notification(
            email_address=os.getenv("NOTIFY_EDITORIAL_ADDRESS"),
            template_id=os.getenv("NOTIFY_UPDATED_JUDGMENT_TEMPLATE_ID"),
            personalisation=personalisation,
        )
        print(f"Sent update notification to {os.getenv('NOTIFY_EDITORIAL_ADDRESS')} (Message ID: {response['id']})")

    def send_new_judgment_notification(self) -> None:
        doctype = "Press Summary" if "/press-summary/" in self.uri else "Judgment"

        personalisation = personalise_email(self.uri, self.metadata)
        personalisation["doctype"] = doctype

        if os.getenv("ROLLBAR_ENV") != "prod":
            print(f"Would send a notification but we're not in production.\n{personalisation}")
            return
        notifications_client = NotificationsAPIClient(os.environ["NOTIFY_API_KEY"])
        response = notifications_client.send_email_notification(
            email_address=os.getenv("NOTIFY_EDITORIAL_ADDRESS"),
            template_id=os.getenv("NOTIFY_NEW_JUDGMENT_TEMPLATE_ID"),
            personalisation=personalisation,
        )
        print(f"Sent new notification to {os.getenv('NOTIFY_EDITORIAL_ADDRESS')} (Message ID: {response['id']})")

    def send_bulk_judgment_notification(self) -> None:
        # Not yet implemented. We currently only autopublish judgments sent in bulk.
        pass

    def unpublish_updated_judgment(self) -> None:
        api_client.set_published(self.uri, False)

    def store_metadata(self) -> None:
        tdr_metadata = self.metadata["parameters"]["TDR"]

        # Store source information
        api_client.set_property(
            self.uri,
            name="source-organisation",
            value=tdr_metadata["Source-Organization"],
        )
        api_client.set_property(self.uri, name="source-name", value=tdr_metadata["Contact-Name"])
        api_client.set_property(self.uri, name="source-email", value=tdr_metadata["Contact-Email"])
        # Store TDR data
        api_client.set_property(
            self.uri,
            name="transfer-consignment-reference",
            value=tdr_metadata["Internal-Sender-Identifier"],
        )
        api_client.set_property(
            self.uri,
            name="transfer-received-at",
            value=tdr_metadata["Consignment-Completed-Datetime"],
        )

    def save_files_to_s3(self) -> None:
        sqs_client, s3_client = aws_clients()
        # Determine if there's a word document -- we need to know before we save the tar.gz file
        docx_filename = extract_docx_filename(self.metadata, self.consignment_reference)
        print(f"extracted docx filename is {docx_filename!r}")

        # Copy original tarfile
        modified_targz_filename = (
            self.local_tar_filename if docx_filename else modify_filename(self.local_tar_filename, "_nodocx")
        )
        with open(self.local_tar_filename, mode="rb") as local_tar:
            store_file(
                file=local_tar,
                destination_folder=S3PrefixString(self.uri + "/"),
                destination_filename=os.path.basename(modified_targz_filename),
                s3_client=s3_client,
            )
        print(f"saved tar.gz as {modified_targz_filename!r}")

        # Store docx and rename
        # The docx_filename is None for files which have been reparsed.
        if docx_filename is not None:
            with tarfile.open(self.local_tar_filename, mode="r") as tar:
                copy_file(
                    tar,
                    f"{self.consignment_reference}/{docx_filename}",
                    f"{self.uri.replace('/', '_')}.docx",
                    S3PrefixString(self.uri + "/"),
                    s3_client,
                )

        # Store parser log
        with suppress(FileNotFoundException), tarfile.open(self.local_tar_filename, mode="r") as tar:
            copy_file(
                tar,
                f"{self.consignment_reference}/parser.log",
                "parser.log",
                S3PrefixString(self.uri + "/"),
                s3_client,
            )

        # Store images
        image_list = self.metadata["parameters"]["TRE"]["payload"]["images"]
        if image_list:
            for image_filename in image_list:
                with tarfile.open(self.local_tar_filename, mode="r") as tar:
                    copy_file(
                        tar,
                        f"{self.consignment_reference}/{image_filename}",
                        image_filename,
                        S3PrefixString(self.uri + "/"),
                        s3_client,
                    )

    @property
    def metadata_object(self) -> Metadata:
        return Metadata(self.metadata)

    def will_publish(self) -> bool:
        originator = self.message.originator
        # TDR
        if originator == "TDR":
            return False

        # Bulk
        if originator == "FCL S3":
            return self.metadata_object.force_publish is True

        # reparse
        if originator == "FCL":
            return api_client.get_published(self.uri) is True

        raise RuntimeError(f"Didn't recognise originator {originator!r}")

    def send_email(self) -> None:
        originator = self.message.originator
        if originator == "FCL":
            return None

        if originator == "FCL S3":
            return None if self.metadata_object.force_publish else self.send_bulk_judgment_notification()

        if originator == "TDR":
            return self.send_new_judgment_notification() if self.inserted else self.send_updated_judgment_notification()

        raise RuntimeError(f"Didn't recognise originator {originator!r}")

    def upload_xml(self) -> None:
        self.updated = self.update_document_xml()
        self.inserted = False if self.updated else self.insert_document_xml()
        if not self.updated and not self.inserted:
            raise DocumentInsertionError(
                f"Judgment {self.uri} failed to insert into Marklogic. Consignment Ref: {self.consignment_reference}",
            )
        self.set_document_identifiers()

    @property
    def upload_state(self) -> str:
        return "updated" if self.updated else "inserted"


def process_message(message):
    """This is the core function -- take a message and ingest the referred-to contents"""

    sqs_client, s3_client = aws_clients()
    ingest = Ingest(message)

    # Extract and parse the judgment XML
    ingest.upload_xml()
    print(f"{ingest.upload_state.title()} judgment xml for {ingest.uri}")

    ingest.send_email()

    # Store metadata in Marklogic
    has_TDR_data = "TDR" in ingest.metadata["parameters"]
    if has_TDR_data:
        ingest.store_metadata()

    # save files to S3
    ingest.save_files_to_s3()

    if ingest.will_publish():
        print(f"publishing {ingest.consignment_reference} at {ingest.uri}")
        api_client.set_published(ingest.uri, True)
        update_published_documents(ingest.uri, s3_client)
    else:
        ingest.unpublish_updated_judgment()

    print("Ingestion complete")
    return message.message


@rollbar.lambda_function
def handler(event, context):
    for message in all_messages(event):
        process_message(message)
