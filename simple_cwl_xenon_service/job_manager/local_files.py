from .cwl import get_files_from_binding

from .job_state import JobState

import json
import os
import requests
import shutil
import urllib

class LocalFiles:
    def __init__(self, job_store, local_config):
        """Create a LocalFiles object.
        Sets up local directory structure as well.

        Local configuration consists of the keys 'store-location-service'
        and 'store-location-client'. The former should contain a str with
        a file:// URL pointing to the path of the local file store, the
        latter a str with a base URL (file or http) describing the way the
        user sees this location.

        Args:
            local_config (Dict): A dict containing key-value pairs with
                local configuration.
        """
        self._job_store = job_store
        """JobStore: The job store to get jobs from."""

        self._basedir = local_config['store-location-service']
        """str: The local path to the base directory where we store our stuff."""

        self._baseurl = local_config['store-location-client']
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

    def resolve_input(self, job_id):
        """Resolves input (workflow and input files) for a job.

        This function will read the job from the database, add a
        .workflow_content attribute with the contents of the
        referenced file, and return an array of tuples containing
        the input data.

        This function will accept local file:// URLs as well as
        remote http:// URLs.

        Args:
            job_id (str): The id of the job whose input to resolve.

        Returns:
            [Tuple[str, str, bytes]]: One tuple per input file, with
            fields name, location and contents in that order.
        """
        with self._job_store:
            job = self._job_store.get_job(job_id)

            job.workflow_content = self._get_content_from_url(job.workflow)

            inputs = json.loads(job.local_input)
            input_files = []
            for name, location in get_files_from_binding(inputs):
                content = self._get_content_from_url(location)
                input_files.append((name, location, content))

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
        shutil.rmtree(self._to_abs_path('output/' + job_id))

    def publish_job_output(self, job_id, output_files):
        """Write output files to the local output dir for this job.

        Uses the .output_files property of the job to get data, and
        updates its .output property with URLs pointing to the newly
        published files, then sets .output_files to None.

        Args:
            job_id (str): The id of the job whose output to publish.
        """
        with self._job_store:
            job = self._job_store.get_job(job_id)
            if output_files is not None:
                output = json.loads(job.remote_output)
                for output_name, file_name, content in output_files:
                    output_loc = self._write_to_output_file(job_id, file_name, content)
                    output[output_name]['location'] = output_loc
                    output[output_name]['path'] = self._to_abs_path('output/' + job_id + '/' + file_name)

                job.local_output = json.dumps(output)
                job.try_transition(JobState.DESTAGING, JobState.SUCCESS)

    def _get_content_from_url(self, url):
        """Return the content referenced by a URL.

        This function will accept local file:// URLs as well as
        remote http:// URLs.

        Args:
            url (str): The URL to get the content of

        Returns:
            bytes: The contents of the file
        """
        parsed_url = urllib.parse.urlparse(url)

        if parsed_url.scheme == 'file':
            return self._read_from_file(os.path.join('', parsed_url.path))
        elif parsed_url.scheme == 'http':
            return requests.get(url).content
        else:
            raise ValueError('Invalid scheme in input URL: ' + url)

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
