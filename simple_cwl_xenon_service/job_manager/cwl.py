def get_files_from_binding(cwl_binding):
    """Parses a CWL input or output binding an returns a list
    containing name: path pairs. Any non-File objects are
    omitted.

    Args:
        cwl_binding: A dict structure parsed from a JSON CWL binding

    Returns:
        A list of (name, path) tuples
    """
    result = []
    for name, value in cwl_binding.items():
        item_class = None
        try:
            item_class = value.get('class')
        except AttributeError:
            pass
        if item_class and item_class == 'File':
            result.append((name, value['path']))

    return result
