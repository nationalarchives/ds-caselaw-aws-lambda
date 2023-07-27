import json
import os
import re
import shutil
import tarfile
import xml.etree.ElementTree as ET
from unittest.mock import ANY, MagicMock, call, patch

import boto3
import lambda_function
import pytest
from botocore.exceptions import NoCredentialsError
from callee import Contains
from caselawclient.Client import (
    MarklogicCommunicationError,
    MarklogicResourceNotFoundError,
    api_client,
)
from notifications_python_client.notifications import NotificationsAPIClient

TDR_TARBALL_PATH = os.path.join(
    os.path.dirname(__file__),
    "../aws_examples/s3/te-editorial-out-int/TDR-2022-DNWR.tar.gz",
)

v1_message_raw = """{"consignment-reference": "DXW-2001-DRGN",
                      "number-of-retries": 1,
                      "consignment-type": "judgment",
                      "s3-folder-url": "DXW-2001-DRGN"}"""

v2_message_raw = """
    {
        "properties": {
            "messageType":
                "uk.gov.nationalarchives.tre.messages.judgmentpackage.available.JudgmentPackageAvailable",
            "timestamp": "2023-05-15T09:14:53.791409Z",
            "function": "staging-tre-judgment-packer-lambda",
            "producer": "TRE",
            "executionId": "cc46e39f-76ef-43c9-a6d7-c6b064c3556a",
            "parentExecutionId": "d26458ae-19a7-4159-8381-805075163198"
        },
        "parameters": {
            "status": "JUDGMENT_PARSE_NO_ERRORS",
            "reference": "FCL-12345",
            "originator": "FCL",
            "bundleFileURI": "http://172.17.0.2:4566/te-editorial-out-int/TDR-2022-DNWR.tar.gz",
            "metadataFilePath": "/metadata.json",
            "metadataFileType": "Json"
        }
    }
    """

v2_message = json.loads(v2_message_raw)


def create_fake_tdr_file(*args, **kwargs):
    shutil.copyfile(TDR_TARBALL_PATH, "/tmp/FCL-12345.tar.gz")


class TestHandler:
    @patch("lambda_function.api_client")
    @patch("lambda_function.extract_metadata")
    @patch("lambda_function.tarfile")
    @patch("lambda_function.boto3.session.Session")
    @patch("lambda_function.urllib3.PoolManager")
    @patch("lambda_function.extract_uri", return_value="source/uri")
    @patch("lambda_function.select_uri", return_value="target/uri")
    def test_handler_messages_v1(
        self,
        select,
        extract,
        urllib_pool,
        boto_session,
        tarfile,
        metadata,
        apiclient,
        capsys,
    ):
        """Mostly intended as a very sketchy test of the primary function"""
        urllib_pool.return_value.request.return_value.status = 200
        urllib_pool.return_value.request.return_value.data = b"data"
        tarfile.open.return_value.getmembers().return_value.name.extractfile.return_value = (
            b"3"
        )

        message = v1_message_raw
        event = {"Records": [{"Sns": {"Message": message}}]}
        lambda_function.handler(event=event, context=None)

        log = capsys.readouterr().out
        assert "Ingester Start: Consignment reference DXW-2001-DRGN" in log
        assert "v1: True" in log
        assert "Ingesting document source/uri at target/uri" in log
        assert "Inserted judgment xml for source/uri at target/uri" in log
        assert "Upload Successful target/uri/target_uri.docx" in log
        assert "Ingestion complete" in log

    @patch("lambda_function.api_client")
    @patch("lambda_function.extract_metadata")
    @patch("lambda_function.tarfile")
    @patch("lambda_function.boto3.session.Session")
    @patch("lambda_function.urllib3.PoolManager")
    @patch("lambda_function.extract_uri", return_value="source/uri")
    @patch("lambda_function.select_uri", return_value="target/uri")
    def test_handler_messages_v2(
        self,
        select,
        extact,
        urllib_pool,
        boto_session,
        tarfile,
        metadata,
        apiclient,
        capsys,
    ):
        """Mostly intended as a very sketchy test of the primary function"""
        urllib_pool.return_value.request.return_value.status = 200
        urllib_pool.return_value.request.return_value.data = b"data"
        tarfile.open.return_value.getmembers().return_value.name.extractfile.return_value = (
            b"3"
        )
        boto_session.return_value.client.return_value.download_file = (
            create_fake_tdr_file
        )
        metadata.return_value = {
            "parameters": {
                "TRE": {"payload": {"xml": "", "filename": "temp.docx", "images": []}},
                "TDR": {
                    "Source-Organization": "",
                    "Contact-Name": "",
                    "Contact-Email": "",
                    "Internal-Sender-Identifier": "",
                    "Consignment-Completed-Datetime": "",
                },
                "PARSER": {"uri": ""},
            }
        }

        # metadata.return_value = {
        #     "uri": "https://caselaw.nationalarchives.gov.uk/id/eat/2022/1",
        #     "court": "EAT",
        #     "cite": "[2022] EAT 1",
        #     "date": "2021-09-28",
        #     "name": "SECRETARY OF STATE FOR JUSTICE v MR ALAN JOHNSON",
        #     "attachments": [],
        # }

        message = v2_message_raw
        event = {"Records": [{"Sns": {"Message": message}}]}
        lambda_function.handler(event=event, context=None)

        log = capsys.readouterr().out
        assert "Ingester Start: Consignment reference FCL-12345" in log
        assert "v1: False" in log
        assert "tar.gz saved locally as /tmp/FCL-12345.tar.gz" in log
        assert "Ingesting document source/uri at target/uri" in log
        assert "Inserted judgment xml for source/uri at target/uri" in log
        assert "Upload Successful target/uri/target_uri.docx" in log
        assert "Ingestion complete" in log


class TestLambda:
    TDR_TARBALL_PATH = os.path.join(
        os.path.dirname(__file__),
        "../aws_examples/s3/te-editorial-out-int/TDR-2022-DNWR.tar.gz",
    )

    EDGE_TARBALL_PATH = os.path.join(
        os.path.dirname(__file__),
        "../aws_examples/s3/te-editorial-out-int/ewca_civ_2021_1881.tar.gz",
    )

    TARBALL_MISSING_METADATA_PATH = os.path.join(
        os.path.dirname(__file__),
        "../aws_examples/s3/te-editorial-out-int/TAR-MISSING-METADATA.tar.gz",
    )

    TARBALL_INVALID_XML_PATH = os.path.join(
        os.path.dirname(__file__),
        "../aws_examples/s3/te-editorial-out-int/TAR-INVALID-XML.tar.gz",
    )

    def test_extract_xml_file_success_tdr(self):
        filename = "TDR-2022-DNWR.xml"
        tar = tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        )
        result = lambda_function.extract_xml_file(tar, filename)
        xml = ET.XML(result.read())
        assert xml.tag == "{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}akomaNtoso"

    def test_extract_xml_file_success_edge(self):
        filename = "judgment.xml"
        tar = tarfile.open(
            self.EDGE_TARBALL_PATH,
            mode="r",
        )
        result = lambda_function.extract_xml_file(tar, filename)
        # XML document may not be valid in an "edge" tarball, so just check the file is there
        assert result is not None

    def test_extract_xml_file_not_found_tdr(self):
        filename = "unknown.xml"
        tar = tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        )
        result = lambda_function.extract_xml_file(tar, filename)
        assert result is None

    def test_extract_xml_file_not_found_edge(self):
        filename = "unknown.xml"
        tar = tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        )
        result = lambda_function.extract_xml_file(tar, filename)
        assert result is None

    def test_extract_xml_file_name_empty(self):
        filename = ""
        tar = tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        )
        result = lambda_function.extract_xml_file(tar, filename)
        assert result is None

    def test_extract_metadata_success_tdr(self):
        consignment_reference = "TDR-2022-DNWR"
        tar = tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        )
        result = lambda_function.extract_metadata(tar, consignment_reference)
        assert result["parameters"]["TRE"]["payload"] is not None

    def test_extract_metadata_success_edge(self):
        consignment_reference = "name_of_tarfile"
        tar = tarfile.open(
            self.EDGE_TARBALL_PATH,
            mode="r",
        )
        result = lambda_function.extract_metadata(tar, consignment_reference)
        assert result["parameters"]["TRE"]["payload"] is not None

    def test_extract_metadata_not_found_tdr(self):
        consignment_reference = "unknown_consignment_reference"
        tar = tarfile.open(
            self.TARBALL_MISSING_METADATA_PATH,
            mode="r",
        )
        with pytest.raises(
            lambda_function.FileNotFoundException, match="Consignment Ref:"
        ):
            lambda_function.extract_metadata(tar, consignment_reference)

    def test_extract_uri_success(self):
        metadata = {
            "parameters": {
                "PARSER": {
                    "uri": "https://caselaw.nationalarchives.gov.uk/id/ewca/civ/2022/111"
                }
            }
        }
        assert lambda_function.extract_uri(metadata, "anything") == "ewca/civ/2022/111"

    def test_extract_uri_incompete(self):
        metadata = {
            "parameters": {
                "PARSER": {"uri": "https://caselaw.nationalarchives.gov.uk/id/"}
            }
        }
        assert lambda_function.extract_uri(metadata, "anything") == "failures/anything"

    def test_extract_uri_missing_key(self):
        metadata = {"parameters": {"PARSER": {}}}
        assert lambda_function.extract_uri(metadata, "anything") == "failures/anything"

    def test_extract_uri_none(self):
        metadata = {"parameters": {"PARSER": {"uri": None}}}
        assert lambda_function.extract_uri(metadata, "anything") == "failures/anything"

    def test_extract_docx_filename_success(self):
        metadata = {"parameters": {"TRE": {"payload": {"filename": "judgment.docx"}}}}
        assert (
            lambda_function.extract_docx_filename(metadata, "anything")
            == "judgment.docx"
        )

    def test_extract_docx_filename_failure(self):
        metadata = {"parameters": {"TRE": {"payload": {}}}}
        with pytest.raises(lambda_function.DocxFilenameNotFoundException):
            lambda_function.extract_docx_filename(metadata, "anything")

    def test_store_metadata(self):
        metadata = {
            "parameters": {
                "TDR": {
                    "Source-Organization": "Ministry of Justice",
                    "Contact-Name": "Tom King",
                    "Internal-Sender-Identifier": "TDR-2021-CF6L",
                    "Consignment-Completed-Datetime": "2021-12-16T14:54:06Z",
                    "Contact-Email": "someone@example.com",
                }
            }
        }

        api_client.set_property = MagicMock()
        lambda_function.store_metadata("uri", metadata)
        calls = [
            call("uri", name="source-organisation", value="Ministry of Justice"),
            call("uri", name="source-name", value="Tom King"),
            call("uri", name="source-email", value="someone@example.com"),
            call("uri", name="transfer-consignment-reference", value="TDR-2021-CF6L"),
            call("uri", name="transfer-received-at", value="2021-12-16T14:54:06Z"),
        ]
        api_client.set_property.assert_has_calls(calls)

    @patch("builtins.print")
    def test_store_file_success(self, mock_print):
        session = boto3.Session
        session.upload_fileobj = MagicMock()
        lambda_function.store_file(None, "folder", "filename.ext", session)
        mock_print.assert_called_with("Upload Successful folder/filename.ext")
        session.upload_fileobj.assert_called_with(None, None, "folder/filename.ext")

    @patch("builtins.print")
    def test_store_file_file_not_found(self, mock_print):
        session = boto3.Session
        session.upload_fileobj = MagicMock(side_effect=FileNotFoundError)
        lambda_function.store_file(None, "folder", "filename.ext", session)
        mock_print.assert_called_with("The file folder/filename.ext was not found")
        session.upload_fileobj.assert_called_with(None, None, "folder/filename.ext")

    @patch("builtins.print")
    def test_store_file_file_no_credentials(self, mock_print):
        session = boto3.Session
        session.upload_fileobj = MagicMock(side_effect=NoCredentialsError)
        lambda_function.store_file(None, "folder", "filename.ext", session)
        mock_print.assert_called_with("Credentials not available")
        session.upload_fileobj.assert_called_with(None, None, "folder/filename.ext")

    @patch.dict(
        os.environ,
        {
            "NOTIFY_API_KEY": "ingester-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "EDITORIAL_UI_BASE_URL": "http://editor.url/",
            "NOTIFY_EDITORIAL_ADDRESS": "test@notifications.service.gov.uk",
            "NOTIFY_NEW_JUDGMENT_TEMPLATE_ID": "template-id",
            "ROLLBAR_ENV": "prod",
        },
        clear=True,
    )
    @patch("builtins.print")
    def test_send_new_judgment_notification(self, mock_print):
        metadata = {
            "parameters": {
                "TDR": {
                    "Source-Organization": "Ministry of Justice",
                    "Contact-Name": "Tom King",
                    "Internal-Sender-Identifier": "TDR-2021-CF6L",
                    "Consignment-Completed-Datetime": "2021-12-16T14:54:06Z",
                    "Contact-Email": "someone@example.com",
                }
            }
        }
        expected_personalisation = {
            "url": "http://editor.url/detail?judgment_uri=uri",
            "consignment": "TDR-2021-CF6L",
            "submitter": "Tom King, Ministry of Justice <someone@example.com>",
            "submitted_at": "2021-12-16T14:54:06Z",
        }
        NotificationsAPIClient.send_email_notification = MagicMock()
        lambda_function.send_new_judgment_notification("uri", metadata)
        NotificationsAPIClient.send_email_notification.assert_called_with(
            email_address="test@notifications.service.gov.uk",
            template_id="template-id",
            personalisation=expected_personalisation,
        )
        mock_print.assert_called_with(
            Contains("Sent notification to test@notifications.service.gov.uk")
        )

    @patch.dict(
        os.environ,
        {"ROLLBAR_ENV": "staging"},
        clear=True,
    )
    @patch("builtins.print")
    def test_do_not_send_new_judgment_notification_on_staging(self, mock_print):
        metadata = {"parameters": {"anything": "anything"}}
        NotificationsAPIClient.send_email_notification = MagicMock()
        lambda_function.send_new_judgment_notification("uri", metadata)
        NotificationsAPIClient.send_email_notification.assert_not_called
        mock_print.assert_not_called

    @patch.object(lambda_function, "store_file")
    def test_copy_file_success(self, mock_store_file):
        tar = tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        )
        filename = "TDR-2022-DNWR/TDR-2022-DNWR.xml"
        session = boto3.Session
        lambda_function.store_file = MagicMock()
        lambda_function.copy_file(tar, filename, "new_filename", "uri", session)
        lambda_function.store_file.assert_called_with(ANY, ANY, ANY, ANY)

    def test_copy_file_not_found(self):
        tar = tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        )
        filename = "does_not_exist.txt"
        session = boto3.Session
        with pytest.raises(lambda_function.FileNotFoundException):
            lambda_function.copy_file(tar, filename, "new_filename", "uri", session)

    @patch.dict(
        os.environ,
        {
            "MAX_RETRIES": "1",
            "SQS_QUEUE_URL": "http://172.17.0.2:4566/000000000000/retry-queue",
        },
    )
    def test_send_retry_message_success_v1(self):
        message = {
            "consignment-reference": "TDR-2022-DNWR",
            "s3-folder-url": "http://172.17.0.2:4566/te-editorial-out-int/TDR-2022-DNWR.tar.gz",
            "consignment-type": "judgment",
            "number-of-retries": 0,
        }
        sqs_client = boto3.Session
        sqs_client.send_message = MagicMock()
        lambda_function.send_retry_message(message, sqs_client)
        expected_message = (
            '{"consignment-reference": "TDR-2022-DNWR", "s3-folder-url": "", '
            '"consignment-type": "judgment", "number-of-retries": 1}'
        )
        sqs_client.send_message.assert_called_with(
            QueueUrl="http://172.17.0.2:4566/000000000000/retry-queue",
            MessageBody=expected_message,
        )

    @pytest.mark.skip
    def test_send_retry_message_success_v2(self):
        ...

    @patch.dict(
        os.environ,
        {"MAX_RETRIES": "1"},
    )
    def test_send_retry_message_failure_v1(self):
        message = {
            "consignment-reference": "TDR-2022-DNWR",
            "s3-folder-url": "http://172.17.0.2:4566/te-editorial-out-int/TDR-2022-DNWR.tar.gz",
            "consignment-type": "judgment",
            "number-of-retries": 1,
        }
        sqs_client = boto3.Session
        with pytest.raises(lambda_function.MaximumRetriesExceededException):
            lambda_function.send_retry_message(message, sqs_client)

    @pytest.mark.skip
    def test_send_retry_message_failure_v2(self):
        ...

    def test_create_xml_contents_success(self):
        tar = tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        )
        result = lambda_function.create_parser_log_xml(tar)
        assert result == "<error>This is the parser error log.</error>"

    @patch.object(tarfile, "open")
    def test_create_xml_contents_failure(self, mock_open_tarfile):
        tar = tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        )
        tar.extractfile = MagicMock(side_effect=KeyError)
        result = lambda_function.create_parser_log_xml(tar)
        assert result == "<error>parser.log not found</error>"

    def test_get_consignment_reference_success_v1(self):
        message = {
            "consignment-reference": "TDR-2022-DNWR",
            "s3-folder-url": "http://172.17.0.2:4566/te-editorial-out-int/ewca_civ_2021_1881.tar.gz",
        }
        result = lambda_function.get_consignment_reference(message)
        assert result == "TDR-2022-DNWR"

    def test_get_consignment_reference_success_v2(self):
        message = v2_message
        message["parameters"]["reference"] = "THIS_REF"
        result = lambda_function.get_consignment_reference(message)
        assert result == "THIS_REF"

    def test_get_consignment_reference_empty_v1(self):
        message = {
            "consignment-reference": "",
            "s3-folder-url": "http://172.17.0.2:4566/te-editorial-out-int/ewca_civ_2021_1881.tar.gz",
        }
        result = lambda_function.get_consignment_reference(message)
        assert result == "ewca_civ_2021_1881"

    def test_get_consignment_reference_empty_v2(self):
        message = v2_message
        message["parameters"]["reference"] = ""
        message["parameters"][
            "bundleFileURI"
        ] = "http://172.17.0.2:4566/te-editorial-out-int/ewca_civ_2021_1881.tar.gz"
        with pytest.raises(lambda_function.InvalidMessageException):
            lambda_function.get_consignment_reference(message)

    def test_get_consignment_reference_missing_v1(self):
        message = {
            "s3-folder-url": "http://172.17.0.2:4566/te-editorial-out-int/ewca_civ_2021_1881.tar.gz"
        }
        result = lambda_function.get_consignment_reference(message)
        assert result == "ewca_civ_2021_1881"

    def test_get_consignment_reference_missing_v2(self):
        message = dict(v2_message)
        del message["parameters"]["reference"]
        message["parameters"][
            "bundleFileURI"
        ] = "http://172.17.0.2:4566/te-editorial-out-int/ewca_civ_2021_1881.tar.gz"
        with pytest.raises(lambda_function.InvalidMessageException):
            lambda_function.get_consignment_reference(message)

    def test_get_consignment_reference_presigned_url_v1(self):
        message = {
            "consignment-reference": "",
            "s3-folder-url": "http://172.17.0.2:4566/te-editorial-out-int/"
            "ewca_civ_2021_1881.tar.gz?randomstuffafterthefilename",
        }
        result = lambda_function.get_consignment_reference(message)
        assert result == "ewca_civ_2021_1881"

    def test_get_consignment_reference_presigned_url_v2(self):
        message = v2_message
        message["parameters"]["reference"] = ""
        message["parameters"][
            "bundleFileURI"
        ] = "http://172.17.0.2:4566/te-editorial-out-int/ewca_civ_2021_1881.tar.gz?randomstuffafterthefilename"
        with pytest.raises(lambda_function.InvalidMessageException):
            lambda_function.get_consignment_reference(message)

    def test_malformed_message(self):
        message = {"something-unexpected": "???"}
        with pytest.raises(lambda_function.InvalidMessageException):
            lambda_function.get_consignment_reference(message)

    def test_update_judgment_xml_success(self):
        xml = ET.XML("<xml>Here's some xml</xml>")
        api_client.get_judgment_xml = MagicMock(return_value=True)
        api_client.save_judgment_xml = MagicMock(return_value=True)
        result = lambda_function.update_judgment_xml("a/fake/uri", xml)
        assert result is True

    def test_update_judgment_xml_judgment_does_not_exist(self):
        xml = ET.XML("<xml>Here's some xml</xml>")
        api_client.get_judgment_xml = MagicMock(
            side_effect=MarklogicResourceNotFoundError("error")
        )
        api_client.save_judgment_xml = MagicMock(return_value=True)
        result = lambda_function.update_judgment_xml("a/fake/uri", xml)
        assert result is False

    def test_update_judgment_xml_judgment_does_not_save(self):
        xml = ET.XML("<xml>Here's some xml</xml>")
        api_client.get_judgment_xml = MagicMock(return_value=True)
        api_client.save_judgment_xml = MagicMock(
            side_effect=MarklogicCommunicationError("error")
        )
        result = lambda_function.update_judgment_xml("a/fake/uri", xml)
        assert result is False

    def test_insert_document_xml_success(self):
        xml = ET.XML("<xml>Here's some xml</xml>")
        api_client.insert_document_xml = MagicMock(return_value=True)
        result = lambda_function.insert_document_xml("a/fake/uri", xml)
        assert result is True

    def test_insert_document_xml_failure(self):
        xml = ET.XML("<xml>Here's some xml</xml>")
        api_client.insert_document_xml = MagicMock(
            side_effect=MarklogicCommunicationError("error")
        )
        result = lambda_function.insert_document_xml("a/fake/uri", xml)
        assert result is False

    def test_get_best_xml_with_valid_xml_file(self):
        filename = "TDR-2022-DNWR.xml"
        with tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        ) as tar:

            result = lambda_function.get_best_xml(
                "a/valid/uri", tar, filename, "a_consignment_reference"
            )
            assert result.__class__ == ET.Element
            assert (
                result.tag
                == "{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}akomaNtoso"
            )

    def test_get_best_xml_with_invalid_xml_file(self):
        filename = "TDR-2022-DNWR.xml"
        with tarfile.open(
            self.TARBALL_INVALID_XML_PATH,
            mode="r",
        ) as tar:
            result = lambda_function.get_best_xml(
                "a/valid/uri", tar, filename, "a_consignment_reference"
            )
            assert result.__class__ == ET.Element
            assert result.tag == "error"

    def test_get_best_xml_with_failure_uri_but_valid_xml(self):
        filename = "TDR-2022-DNWR.xml"
        with tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        ) as tar:
            result = lambda_function.get_best_xml(
                "failures/consignment_reference",
                tar,
                filename,
                "a_consignment_reference",
            )
            assert result.__class__ == ET.Element
            assert (
                result.tag
                == "{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}akomaNtoso"
            )

    def test_get_best_xml_with_failure_uri_and_missing_xml(self):
        filename = "missing_filename.xml"
        with tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        ) as tar:
            result = lambda_function.get_best_xml(
                "failures/consignment_reference",
                tar,
                filename,
                "a_consignment_reference",
            )
            assert result.__class__ == ET.Element
            assert result.tag == "error"

    def test_get_best_xml_with_no_xml_file(self):
        filename = "missing_filename.xml"
        with tarfile.open(
            self.TDR_TARBALL_PATH,
            mode="r",
        ) as tar:
            result = lambda_function.get_best_xml(
                "failures/consignment_reference",
                tar,
                filename,
                "a_consignment_reference",
            )
            assert result.__class__ == ET.Element
            assert result.tag == "error"

    def test_unpublish_updated_judgment(self):
        uri = "a/fake/uri"
        api_client.set_published = MagicMock()
        lambda_function.unpublish_updated_judgment(uri)
        api_client.set_published.assert_called_with(uri, False)

    @patch("lambda_function.api_client")
    def test_change_uri_if_taken(self, mock_api):
        mock_api.judgment_exists.return_value = True
        collision_1 = lambda_function.select_uri("")
        collision_2 = lambda_function.select_uri("")
        # it looks the right shape
        assert re.search(r"collisions/.*T.*/[\dabcdef-]+", collision_1)
        # it has a different ending
        assert collision_1.split("/")[-1] != collision_2.split("/")[-1]

    @patch("lambda_function.api_client")
    def test_dont_change_uri_if_free(self, mock_api):
        mock_api.judgment_exists.return_value = False
        assert lambda_function.select_uri("teststring") == "teststring"
