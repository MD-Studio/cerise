class InputFile:
    def __init__(self, name, location, content, secondary_files, index=None):
        """Create an InputFile object.

        This describes an input file, and is the result of resolving \
        input files from the user-submitted input description. It is \
        used by the staging machinery to stage these files, and \
        update the input description with remote paths.

        Args:
            name (str): The name of the input for which this file is.
            location (str): A URL with the (local) location of the \
                    file.
            content (bytes): The content of the file.
            secondary_files ([InputFile]): A list of secondary files.
        """
        self.name = name
        """(str) The input name for which this file is."""
        self.index = index
        """(int) The index of this file, if it is in an array of files."""
        self.location = location
        """(str) Local URL of the file."""
        self.content = content
        """(bytes) The content of the file."""
        self.secondary_files = secondary_files
        """([InputFile]) CWL secondary files."""
