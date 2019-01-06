from cerise.back_end.cwl import get_files_from_binding
from cerise.back_end.file import File

from cerise.job_store.job_state import JobState
from cerise.job_store.sqlite_job_store import SQLiteJobStore
from cerise.config import Config

import cerulean
import json
import logging
import os
import requests
import shutil
from typing import cast, List, Tuple, Optional
import urllib


ConnectionError = requests.exceptions.ConnectionError


class LocalFiles:
    def __init__(self, job_store: SQLiteJobStore, config: Config) -> None:
        """Create a LocalFiles object.
        Sets up local directory structure as well.

        Args:
            job_store: The job store to use
            config: The configuration.
        """
        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
        self._job_store = job_store
        """JobStore: The job store to get jobs from."""

        self._basedir = config.get_store_location_service()
        """cerulean.Path: The directory used to exchange data with the client."""

        self._baseurl = config.get_store_location_client()
        """str: The externally accessible base URL corresponding to the _basedir."""

        self._basedir.mkdir(exists_ok=True)
        (self._basedir / 'input').mkdir(exists_ok=True)
        (self._basedir / 'output').mkdir(exists_ok=True)

    def resolve_secondary_files(self, secondary_files: List[File]) -> None:
        """Makes a File object for each secondary file.

        Works recursively, so nested secondaryFiles work.

        Args:
            secondary_files: List of secondary files.

        Returns:
            Resulting Files, with contents.
        """
        for secondary_file in secondary_files:
            self._logger.debug("Resolving secondary file from " + secondary_file.location)
            secondary_file.content = self._get_content_from_url(secondary_file.location)
            self.resolve_secondary_files(secondary_file.secondary_files)


    def resolve_input(self, job_id: str) -> List[File]:
        """Resolves input (workflow and input files) for a job.

        This function will read the job from the database, add a
        .workflow_content attribute with the contents of the
        workflow, and return a list of File objects describing the
        input files.


        This function will accept local file:// URLs as well as
        remote http:// URLs.

        Args:
            job_id: The id of the job whose input to resolve.

        Returns:
            A list of File objects to stage.
        """
        self._logger.debug("Resolving input for job " + job_id)
        with self._job_store:
            job = self._job_store.get_job(job_id)

            job.workflow_content = self._get_content_from_url(job.workflow)

            inputs = json.loads(job.local_input)
            input_files = get_files_from_binding(inputs)
            for input_file in input_files:
                self._logger.debug("Resolving file for input {} from {}".format(
                    input_file.name, input_file.location))
                input_file.content = self._get_content_from_url(input_file.location)
                self.resolve_secondary_files(input_file.secondary_files)

            return input_files

    def create_output_dir(self, job_id: str) -> None:
        """Create an output directory for a job.

        Args:
            job_id: The id of the job to make a work directory for.
        """
        (self._basedir / 'output' / job_id).mkdir()

    def delete_output_dir(self, job_id: str) -> None:
        """Delete the output directory for a job.
        This will remove the directory and everything in it.

        Args:
            job_id: The id of the job whose output directory to delete.
        """
        job_dir = self._basedir / 'output' / job_id
        if job_dir.is_dir():
            job_dir.rmdir(recursive=True)

    def publish_job_output(self, job_id: str, output_files: List[File]
                           ) -> None:
        """Write output files to the local output dir for this job.

        Uses the .output_files property of the job to get data, and
        updates its .output property with URLs pointing to the newly
        published files, then sets .output_files to None.

        Args:
            job_id: The id of the job whose output to publish.
            output_files: List of output files to publish.
        """
        self._logger.debug("Publishing output for job " + job_id)
        job_dir = self._basedir / 'output' / job_id
        with self._job_store:
            job = self._job_store.get_job(job_id)
            if output_files != []:
                output = json.loads(job.remote_output)
                self.create_output_dir(job_id)
                for outf in output_files:
                    out_file = job_dir / outf.location
                    out_file.write_bytes(cast(bytes, outf.content))

                    output[outf.name]['location'] = self._to_external_url(
                            'output/' + job_id + '/' + outf.location)
                    output[outf.name]['path'] = str(out_file)

                job.local_output = json.dumps(output)

    def _get_content_from_url(self, url: str) -> bytes:
        """Return the content referenced by a URL.

        This function will accept local file:// URLs as well as
        remote http:// URLs. If a URL starts with the client-side
        location of the file exchange store, the service-side location
        is substituted before trying to download the file.

        Args:
            url: The URL to get the content of

        Returns:
            bytes: The contents of the file
        """
        if self._baseurl and url.startswith(self._baseurl):
            source = self._basedir / url[len(self._baseurl):]
            return source.read_bytes()
        else:
            parsed_url = urllib.parse.urlparse(url)
            if parsed_url.scheme == 'file':
                if self._baseurl is None:
                    source = cerulean.LocalFileSystem() / parsed_url.path
                    return source.read_bytes()
                else:
                    raise ValueError('Cerise is configured to only accept'
                                     ' local files from {}'.format(
                                         self._baseurl))
            elif parsed_url.scheme == 'http':
                response = requests.get(url)
                if response.status_code != 200:
                    raise ValueError(('Could not resolve {}, the server'
                                      ' returned {} {}').format(
                                          url, response.status_code,
                                          response.reason))
                return response.content
            else:
                raise ValueError('Invalid scheme {} in input URL: {}'.format(
                        parsed_url.scheme, url))

    def _write_to_output_file(self, job_id: str, rel_path: str, data: bytes
                              ) -> str:
        """Write the data to a local file.

        Args:
            job_id: The id of the job to write data for
            rel_path: A path relative to the job's output directory
            data: The data to write

        Returns:
            An external URL that points to the file
        """
        (self._basedir / 'output' / job_id / rel_path).write_bytes(data)
        return self._to_external_url('output/' + job_id + '/' + rel_path)

    def _to_external_url(self, rel_path: str) -> str:
        return self._baseurl + '/' + rel_path
