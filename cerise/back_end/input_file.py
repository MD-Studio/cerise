from typing import List, Optional


class InputFile:
    def __init__(self, name: Optional[str], location: str,
                 content: Optional[bytes], secondary_files: List['InputFile'],
                 index: int = None) -> None:
        """Create an InputFile object.

        This describes an input file, and is the result of resolving \
        input files from the user-submitted input description. It is \
        used by the staging machinery to stage these files, and \
        update the input description with remote paths.

        Args:
            name: The name of the input for which this file is.
            location: A URL with the (local) location of the \
                    file.
            content: The content of the file.
            secondary_files: A list of secondary files.
        """
        self.name = name
        """The input name for which this file is."""
        self.index = index
        """The index of this file, if it is in an array of files."""
        self.location = location
        """Local URL of the file."""
        self.content = content
        """The content of the file."""
        self.secondary_files = secondary_files
        """CWL secondary files."""
