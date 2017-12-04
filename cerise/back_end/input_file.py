class InputFile:
    def __init__(self, name, location, content):
        """Create an InputFile object.

        This describes an input file, and is the result of resolving \
        input files from the user-submitted input description. It is \
        used by the staging machinery to stage these files, and \
        update the input description with remote paths.

        Args:
            name (str): The name of the input for which this file is.
            location (str): A URL with the (local) location of the \
                    file.
            contents (bytes): The contents of the file.
        """
        self.name = name
        """(str) The input name for which this file is."""
        self.location = location
        """(str) Local URL of the file."""
        self.content = content
        """(bytes) The content of the file."""
