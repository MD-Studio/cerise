from .cwl import get_files_from_binding, get_required_num_cores
from .input_file import InputFile

from cerise.job_store.job_state import JobState

import json
import logging
import os
import requests
import shutil
import urllib

class LocalFiles:
    def __init__(self, job_store, config):
        """Create a LocalFiles object.
        Sets up local directory structure as well.

        Args:
            config (Config): The configuration.
        """
        self._logger = logging.getLogger(__name__)
        """Logger: The logger for this class."""
        self._job_store = job_store
        """JobStore: The job store to get jobs from."""

        self._basedir = config.get_store_location_service()
        """str: The local path to the base directory where we store our stuff."""

        self._baseurl = config.get_store_location_client()
        """str: The externally accessible base URL corresponding to the _basedir."""

        basedir = urllib.parse.urlparse(self._basedir)
        if basedir.scheme != 'file':
            raise ValueError('Invalid scheme in store-location-service: ' + basedir.scheme)
        self._basedir = basedir.path

        try:
            os.mkdir(self._basedir)
        except FileExistsError:
            pass

        try:
            os.mkdir(self._to_abs_path('input'))
        except FileExistsError:
            pass

        try:
            os.mkdir(self._to_abs_path('output'))
        except FileExistsError:
            pass


    def resolve_secondary_files(self, secondary_files):
        """Makes an InputFile object for each secondary file.

        Works recursively, so nested secondaryFiles work.

        Args:
            secondary_files ([InputFile]): List of secondary files.

        Returns:
            ([InputFile]): Resulting InputFiles, with contents.
        """
        for secondary_file in secondary_files:
            self._logger.debug("Resolving secondary file from " + secondary_file.location)
            secondary_file.content = self._get_content_from_url(secondary_file.location)
            self.resolve_secondary_files(secondary_file.secondary_files)


    def resolve_input(self, job_id):
        """Resolves input (workflow and input files) for a job.

        This function will read the job from the database, add a
        .workflow_content attribute with the contents of the
        workflow, and return a list of InputFile objects
        describing the input files.


        This function will accept local file:// URLs as well as
        remote http:// URLs.

        Args:
            job_id (str): The id of the job whose input to resolve.

        Returns:
            [InputFile]: A list of InputFile objects to stage.
        """
        self._logger.debug("Resolving input for job " + job_id)
        with self._job_store:
            job = self._job_store.get_job(job_id)

            job.workflow_content = self._get_content_from_url(job.workflow)
            job.required_num_cores = get_required_num_cores(job.workflow_content)

            inputs = json.loads(job.local_input)
            input_files = get_files_from_binding(inputs)
            for input_file in input_files:
                self._logger.debug("Resolving file for input {} from {}".format(
                    input_file.name, input_file.location))
                input_file.content = self._get_content_from_url(input_file.location)
                self.resolve_secondary_files(input_file.secondary_files)

            return input_files

    def create_output_dir(self, job_id):
        """Create an output directory for a job.

        Args:
            job_id (str): The id of the job to make a work directory for.
        """
        os.mkdir(self._to_abs_path('output/' + job_id))

    def delete_output_dir(self, job_id):
        """Delete the output directory for a job.
        This will remove the directory and everything in it.

        Args:
            job_id (str): The id of the job whose output directory to delete.
        """
        job_dir = self._to_abs_path('output/' + job_id)
        if os.path.isdir(job_dir):
            shutil.rmtree(job_dir)

    def publish_job_output(self, job_id, output_files):
        """Write output files to the local output dir for this job.

        Uses the .output_files property of the job to get data, and
        updates its .output property with URLs pointing to the newly
        published files, then sets .output_files to None.

        Args:
            job_id (str): The id of the job whose output to publish.
        """
        self._logger.debug("Publishing output for job " + job_id)
        with self._job_store:
            job = self._job_store.get_job(job_id)
            if output_files is not None and output_files != []:
                output = json.loads(job.remote_output)
                self.create_output_dir(job_id)
                for output_name, file_name, content in output_files:
                    output_loc = self._write_to_output_file(job_id, file_name, content)
                    output[output_name]['location'] = output_loc
                    output[output_name]['path'] = self._to_abs_path('output/' + job_id + '/' + file_name)

                job.local_output = json.dumps(output)

    def _get_content_from_url(self, url):
        """Return the content referenced by a URL.

        This function will accept local file:// URLs as well as
        remote http:// URLs. If a URL starts with the client-side
        location of the file exchange store, the service-side location
        is substituted before trying to download the file.

        Args:
            url (str): The URL to get the content of

        Returns:
            bytes: The contents of the file
        """
        if url.startswith(self._baseurl):
            url = 'file://' + self._basedir + url[len(self._baseurl):]

        parsed_url = urllib.parse.urlparse(url)

        if parsed_url.scheme == 'file':
            return self._read_from_file(os.path.join('', parsed_url.path))
        elif parsed_url.scheme == 'http':
            response = requests.get(url)
            if response.status_code != 200:
                raise FileNotFoundError
            return response.content
        else:
            raise ValueError('Invalid scheme {} in input URL: {}'.format(parsed_url.scheme, url))

    def _read_from_file(self, abs_path):
        """Read data from a local file.

        Args:
            abs_path (str): An absolute local path

        Returns:
            bytes: The contents of the file.
        """
        with open(abs_path, 'rb') as f:
            data = f.read()
        return data

    def _write_to_output_file(self, job_id, rel_path, data):
        """Write the data to a local file.

        Args:
            job_id (str): The id of the job to write data for
            rel_path (str): A path relative to the job's output directory
            data (bytes): The data to write

        Returns:
            str: An external URL that points to the file
        """
        with open(self._to_abs_path('output/' + job_id + '/' + rel_path), 'wb') as f:
            f.write(data)

        return self._to_external_url('output/' + job_id + '/' + rel_path)

    def _to_abs_path(self, rel_path):
        return self._basedir + '/' + rel_path

    def _to_external_url(self, rel_path):
        return self._baseurl + '/' + rel_path
