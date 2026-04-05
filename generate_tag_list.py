# generate_tag_list.py

def generate_tag_list(tag_dict):
    """
    Generate a list of tags from the tag dictionary.
    Args:
        tag_dict (dict): Dictionary containing facility information and tags.
    Returns:
        list: List of tags.
    """
    return [tag for attr_dict in tag_dict.values() for tag in attr_dict.values() if tag not in ["facilitycode", "description"]]
