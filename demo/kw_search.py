def search_attribute(api, name: str, limit: int):
    drs_attr = api.search_attribute(name, max_results=limit)
    return drs_attr

def search_content(api, kw:str, limit: int):
    drs_content = api.search_content(kw, max_results=limit)
    return drs_content
