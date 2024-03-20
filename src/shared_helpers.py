def get_file_id(file_path: str) -> str | bool:
    """Converts the file-path of the Azure storage blob to a consistent id, allowing an image and it's derivatives to be tracked throughout the pipeline

    Args:
        file_path (str): blob storage file path

    Returns:
        file_id (str): the identifier for an image and its derivatives
        bool: False value returned when a file path cannot be used to generate a file_id
    """

    file_path_stem = file_path.split(".")
    if len(file_path_stem) > 1:
        file_path_stem = ".".join(file_path_stem[:-1])
    else:
        file_path_stem = ".".join(file_path_stem)

    file_id = file_path_stem.split("/")
    if len(file_id) <= 2:
        return False
    file_id = "/".join(file_id[2:])

    return file_id
