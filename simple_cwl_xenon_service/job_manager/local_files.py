from .cwl import get_files_from_binding
from .job_state import JobState

import json
import os
import shutil
import urllib

class LocalFiles:
    def __init__(self, job_store, local_config):
        """Create a LocalFiles object.
        Sets up local directory structure as well, but refuses to
        create the top-level directory.

        Args:
            local_config: A dict containing key-value pairs with local
                configuration.
        """
        self._job_store = job_store
        """The job store to get jobs from."""

        self._basedir = local_config['file-store-path']
        """The local path to the base directory where we store our stuff."""

        self._baseurl = local_config['file-store-location']
        """The externally accessible base URL corresponding to the _basedir."""

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
        referenced file, and add a .input_files attribute
        containing the input data.

        Local file:// URLs, or URLs without a schema, will be
        resolved relative to the local file-store-path/input.
        This function will also load remote http:// URLs.

        Args:
            job_id: The id of the job whose input to resolve.
        """
        job = self._job_store.get_job(job_id)

        job.workflow_content = self._get_content_from_url(job.workflow)

        inputs = json.loads(job.input)
        job.input_files = []
        for name, location in get_files_from_binding(inputs):
            content = self._get_content_from_url(location)
            job.input_files.append((name, location, content))

    def create_output_dir(self, job_id):
        """Create an output directory for a job.

        Args:
            job_id: The id of the job to make a work directory for.
        """
        os.mkdir(self._to_abs_path('output/' + job_id))

    def delete_output_dir(self, job_id):
        """Delete the output directory for a job.
        This will remove the directory and everything in it.

        Args:
            job_id: The id of the job whose output directory to delete.
        """
        shutil.rmtree(self._to_abs_path('output/' + job_id))

    def publish_job_output(self, job_id):
        """Write output files to the local output dir for this job.

        Uses the .output_files property of the job to get data, and
        updates its .output property with URLs pointing to the newly
        published files, then sets .output_files to None.

        Args:
            job_id: The id of the job whose output to publish.
        """
        job = self._job_store.get_job(job_id)
        if job.output_files is not None:
            output = json.loads(job.output)
            for output_name, file_name, content in job.output_files:
                output_loc = self._write_to_output_file(job_id, file_name, content)
                output[output_name]['location'] = output_loc
                output[output_name]['path'] = self._to_abs_path('output/' + job_id + '/' + file_name)
            job.output = json.dumps(output)
            job.output_files = None

    def publish_all_jobs_output(self):
        """Publish the output of all jobs that have some.
        See publish_job_output() for details.
        """
        for job in self._job_store.list_jobs():
            self.publish_job_output(job.id)

    def _get_content_from_url(self, url):
        """Return the content referenced by a URL.

        URLs with no schema, or a file:// schema, will be
        resolved relative to the file-store-path/input directory,
        remote URLs will be downloaded.

        Args:
            url A str containing the URL to get the content of

        Returns:
            A bytes object with the contents of the file
        """
        parsed_url = urllib.parse.urlparse(url, scheme='file')

        if parsed_url.scheme == 'file':
            return self._read_from_file(parsed_url.path)
        else:
            return requests.get(url).content

    def _read_from_file(self, rel_path):
        """Read data from a local file.

        Args:
            rel_path: A string with a path relative to the local base directory

        Returns:
            A bytes-object containing the contents of the file.
        """
        with open(self._to_abs_path(rel_path), 'rb') as f:
            data = f.read()
        return data

    def _write_to_output_file(self, job_id, rel_path, data):
        """Write the data to a local file.

        Args:
            job_id: The id of the job to write data for
            rel_path: A string with a path relative to the job's output directory
            data: A bytes-object containing the data to write

        Returns:
            A string containing an external URL that points to the file
        """
        with open(self._to_abs_path('output/' + job_id + '/' + rel_path), 'wb') as f:
            f.write(data)

        return self._to_external_url('output/' + job_id + '/' + rel_path)

    def _to_abs_path(self, rel_path):
        return self._basedir + '/' + rel_path

    def _to_external_url(self, rel_path):
        return self._baseurl + '/' + rel_path
