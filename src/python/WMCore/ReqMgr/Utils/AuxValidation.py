"""
Module with validation functions for the Auxiliary-based
RESTful APIs, such as unifiedconfig, wmagentconfig, etc.
"""


def validateUnifiedConfig(data):
    """
    Function to validate content of the Unified configuration
    before it gets persisted in the database.
    :param data: dictionary with the Unified configuration
    :return: raises an exception in case of problems, otherwise None
    """
    SUPPORTED_KEYS = {"tiers_to_DDM", "tiers_no_DDM", "tiers_with_no_custodial"}
    NESTED_KEYS = {"value", "description"}
    baseError = "Unified schema error."
    if not isinstance(data, dict):
        raise RuntimeError(f"{baseError} It needs to be a dictionary, not: {type(data)}")

    # does user data contains all of the required top level parameters?
    if not SUPPORTED_KEYS.issubset(set(data.keys())):
        raise RuntimeError(f"{baseError} It is missing some of the required parameters: {SUPPORTED_KEYS}")

    for keyAttr in data:
        # is the user data parameter listed as a supported parameter?
        if keyAttr not in SUPPORTED_KEYS:
            raise RuntimeError(f"{baseError} It has unsupported parameter: {keyAttr}")

        # does user data contains all of the internal keys within each top level parameter?
        if not NESTED_KEYS.issubset(set(data.get(keyAttr, {}).keys())):
            # are all elements of expected inner keys in data[keyAttr]?
            msg = f"{baseError} Attribute '{keyAttr}' is missing "
            msg += f"some of the nested required keys: {NESTED_KEYS}"
            raise RuntimeError(msg)

        # are the nested keys in the user data parameter provided supported in the schema?
        for nestedAttr, nestedValue in data[keyAttr].items():
            if nestedAttr not in NESTED_KEYS:
                msg = f"{baseError} Attribute '{keyAttr}' has "
                msg += f"unsupported nested key: {nestedAttr}"
                raise RuntimeError(msg)
            # then validate the value data type as well
            if nestedAttr == "value":
                if not isinstance(nestedValue, list):
                    msg = f"{baseError} Data type of {keyAttr}.{nestedAttr} "
                    msg =+ f"should be 'list', not '{type(nestedValue)}'"
                    raise RuntimeError(msg)
            elif nestedAttr == "description":
                if not isinstance(nestedValue, str):
                    msg = f"{baseError} Data type of {keyAttr}.{nestedAttr} "
                    msg =+ f"should be 'str', not '{type(nestedValue)}'"
                    raise RuntimeError(msg)
    # if it got here, then everything is fine!
