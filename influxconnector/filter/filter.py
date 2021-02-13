def simple_json_filter(filter_list, json):
    """ Filter for simple JSON input that removes every dictionary key that is not listed in filter_list

    Arguments:
        filter_list {list} -- List of whitelisting keys
        json {dict} -- Actual json content which is filtered

    Returns:
        {dict} -- List that contains only keys that are covered in filter_list
    """
    return {key: value for key, value in json.items() if key in filter_list}
