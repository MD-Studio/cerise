def get_files_from_binding(cwl_binding):
    """Parses a CWL input or output binding an returns a list
    containing name: path pairs. Any non-File objects are
    omitted.

    Args:
        cwl_binding: A dict structure parsed from a JSON CWL binding

    Returns:
        A list of (name, location) tuples, where name is
        a str containing the input or output name, and
        location a str containing the URL.
    """
    result = []
    for name, value in cwl_binding.items():
        item_class = None
        try:
            item_class = value.get('class')
        except AttributeError:
            pass
        if item_class and item_class == 'File':
            result.append((name, value['location']))

    return result
