from typing import List, Optional

from cerulean import Path


class File:
    def __init__(self, name: Optional[str], index: Optional[int],
                 location: str, secondary_files: List['File']) -> None:
        """Create a File object.

        This describes a file, and is the result of resolving \
        input files from the user-submitted input description, or output \
        generated by the CWL runner. It is used by the staging machinery to \
        stage these files, and update the input description with remote paths.

        Args:
            name: The name of the input for which this file is.
            index: The index of this file into an array of Files.
            location: A URL with the (local) location of the \
                    file.
            secondary_files: A list of secondary files.
        """
        self.name = name
        """The input name for which this file is."""
        self.index = index
        """The index of this file, if it is in an array of files."""
        self.location = location
        """Local URL of the file."""
        self.source = None  # type: Optional[Path]
        """The source of the file."""
        self.secondary_files = secondary_files
        """CWL secondary files."""
