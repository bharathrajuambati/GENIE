# fetch_current_values.py

def fetch_current_values(pi, tag_list):
    """
    Fetch current values of the tags from the AEMO PI system.
    Args:
        pi: The connected AEMO PI object.
        tag_list (list): List of tags to fetch values for.
    Returns:
        DataFrame: DataFrame containing the current values of the tags.
    """
    current_values = pi.get_current_value(tag_list)  # Replace with actual method
    return current_values
