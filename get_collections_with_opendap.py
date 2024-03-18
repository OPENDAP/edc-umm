""" Module to determine all collections that are cloud-hosted and have an
    OPeNDAP RelatedUrl in their first granule.

    Usage:

    ```
    from get_collections_with_opendap import get_collections_with_opendap_urls

    # Fresh run, with no locally saved GraphQL results
    get_collections_with_opendap_urls('prod')

    # Already run the GraphQL queries, want to read from local file:
    get_collections_with_opendap_urls('prod', file_path='all_collections_prod.json')
    ```

    Values for `environment`: `'sit'`, `'uat'`, `'prod'`.

    Requirements:

    * A local `.netrc` file.
    * The `requests` package in your local Python environment.

    To scan _all_ collections, you can comment out the line in the query that
    sets `cloudHosted` to `True`. However, that means scanning 52000 collections
    in production and 99000 collections in UAT. Odd behaviour with incorrect
    pagination has been observed for UAT and all collections, however. To
    reduce repeated runtime, this code will save the output from the GraphQL
    queries to a local file `all_collections_<environment>.json`. This file can
    be used with subsequent runs using the `file_path` kwarg.

    Disclaimer - this will only include collections which the user running the
    module has permission to see.

    Owen Littlejohns, 2024-03-15

"""
import json

import requests


environment_parameters = {
    'prod': {
        'edl_root': 'https://urs.earthdata.nasa.gov',
        'graphql': 'https://graphql.earthdata.nasa.gov/api',
        'hyrax_substring': 'opendap.earthdata.nasa.gov',
    },
    'uat': {
        'edl_root': 'https://uat.urs.earthdata.nasa.gov',
        'graphql': 'https://graphql.uat.earthdata.nasa.gov/api',
        'hyrax_substring': 'opendap.uat.earthdata.nasa.gov',
    },
    'sit': {
        'edl_root': 'https://sit.urs.earthdata.nasa.gov',
        'graphql': 'https://graphql.sit.earthdata.nasa.gov/api',
        'hyrax_substring': 'opendap.sit.earthdata.nasa.gov',
    }
}


def get_edl_token(environment: str) -> str:
    """ Retrieve an EDL token for use in requests to CMR graph. If
        the user identified by a local .netrc file does not have a
        token then a new one will be generated.

    """
    edl_root = environment_parameters.get(environment).get('edl_root')

    existing_tokens_response = requests.get(
        f'{edl_root}/api/users/tokens',
        headers={'Content-type': 'application/json'}
    )
    existing_tokens_response.raise_for_status()
    existing_tokens_json = existing_tokens_response.json()

    if len(existing_tokens_json) == 0:
        new_token_response = requests.post(
            f'{edl_root}/api/users/token',
            headers={'Content-type': 'application/json'}
        )
        new_token_response.raise_for_status()
        new_token_json = new_token_response.json()
        edl_token = new_token_json['access_token']
    else:
        edl_token = existing_tokens_json[0]['access_token']

    return edl_token


def query_cmr_graph_for_collections(
    environment: str,
    edl_bearer_token: str
) -> list[dict]:
    """ Perform a query against CMR graph (https://graphql.earthdata.nasa.gov/api)
        to retrieve cloud hosted collections with granules that have a granule
        with a RelatedUrl of type 'USE SERVICE API', subtype 'OPENDAP DATA' and
        the URL itself points to Hyrax.

        Limit is set to 100 results to avoid timeouts.

    """
    graphql_rooturl = environment_parameters.get(environment).get('graphql')

    query_string = """
    query Collections($collectionParams: CollectionsInput, $granulesParams: GranulesInput) {
      collections(params: $collectionParams) {
        count
        cursor
        items {
          shortName
          version
          conceptId
          granules(params: $granulesParams) {
            items {
              relatedUrls
            }
          }
        }
      }
    }
    """

    query_parameters = {
        'collectionParams': {
            'cloudHosted': True,
            'limit': 100,
        },
        'granulesParams': {
            'limit': 1
        }
    }

    request_json = {
        'operationName': 'Collections',
        'query': query_string,
        'variables': query_parameters
    }


    # Set collection count to a non-zero value to trigger first request:
    collection_count = 1

    # Create an empty list to contain results after pagination:
    all_collections = []

    # Add an error count and limit, to prevent infinite loops from repeated errors:
    error_count = 0
    max_errors = 3

    # Perform paginated request.
    # Requests will continue until the list is the same length as the number of
    # results in CMR graph.
    while error_count < max_errors and len(all_collections) < collection_count:
        cmr_graph_response = requests.post(
            url=graphql_rooturl,
            json=request_json,
            headers={'Authorization': f'Bearer {edl_bearer_token}'}
        )

        if cmr_graph_response.ok:
            json_data = cmr_graph_response.json()

            # Update collection_count to actual value:
            collection_count = json_data['data']['collections']['count']

            # Concatenate new results with existing list:
            all_collections.extend(
                json_data['data']['collections']['items']
            )

            # Update the cursor for the potential next request:
            request_json['variables']['collectionParams']['cursor'] = (
                json_data['data']['collections']['cursor']
            )

            # Print progress:
            print(f'Retrieved {len(all_collections)}/{collection_count} collections')

        else:
            error_count += 1
            print(f'response status code: f{cmr_graph_response.status_code}')
            print(f'response : {cmr_graph_response.content}')

    return all_collections


def get_collection_related_urls(collection: dict) -> list[dict]:
    """ Get the list of RelatedUrls, accounting for collections with no
        granules or granules with no RelatedUrls. In these two cases, return
        an empty list.

    """
    try:
        granules = collection.get('granules', {})
        if granules is None:
            granules = {}

        granules = granules.get('items')
    except AttributeError:
        print(f'Failed: {collection["shortName"]}')
        raise

    if granules == [] or granules is None:
        granules = [{}]

    return granules[0].get('relatedUrls', [])


def collection_has_opendap_url(collection: dict, environment: str) -> bool:
    """ Check the following conditions are met for a single RelatedUrl:

        - RelatedUrl.type == 'USE SERVICE API'
        - RelatedUrl.subtype == 'OPENDAP DATA'
        - RelatedUrl.url contains Hyrax substring
    """
    hyrax_substring = environment_parameters.get(environment).get('hyrax_substring')
    return any(
        related_url.get('type') == 'USE SERVICE API'
        and related_url.get('subtype') == 'OPENDAP DATA'
        and hyrax_substring in related_url.get('url', '')
        for related_url in get_collection_related_urls(collection)
    )


def filter_for_opendap_granules(
    collections: list[dict],
    environment: str
) -> list[dict]:
    """ Go through the list of all collections and retrieve only those that
        have an OPeNDAP RelatedUrl.

    """
    return [
        collection
        for collection in collections
        if collection_has_opendap_url(collection, environment)
    ]


def get_formatted_collection(collection: dict) -> dict:
    """ Create a streamlined object for saving to a JSON file with only the
        collection information.

    """
    return {
        'short_name': collection['shortName'],
        'version': collection['version'],
        'concept_id': collection['conceptId'],
    }


def get_all_collections_from_file(file_path: str) -> list[dict]:
    """ Retrieve results from CMR (in case there are parsing issues later). """
    with open(file_path, 'r', encoding='utf-8') as file_handler:
        all_collections = json.load(file_handler)

    return all_collections


def save_all_collections(
    all_cloud_collections: list[dict],
    environment: str
):
    """ Save intermediate result to avoid querying for 50,000 collections. """
    file_path = f'all_collections_{environment}.json'

    with open(file_path, 'w', encoding='utf-8') as file_handler:
        json.dump(all_cloud_collections, file_handler, indent=2)

    print(f'Saved to {file_path}')


def save_opendap_collections(
    opendap_collections: list[dict],
    environment: str
) -> None:
    """ Output all collections identified as having an OPeNDAP RelatedUrl to a
        JSON output file.

    """
    output_filename = f'opendap_collections_{environment}.json'

    formatted_output = [
        get_formatted_collection(collection)
        for collection in opendap_collections
    ]

    with open(output_filename, 'w', encoding='utf-8') as file_handler:
        json.dump(formatted_output, file_handler, indent=2)


def get_collections_with_opendap_urls(environment: str, file_path: str=None):
    """ Main entry point for the module. """
    environment = environment.lower()

    if environment not in {'sit', 'uat', 'prod'}:
        raise ValueError('environment must be one of: sit, uat or prod')

    edl_bearer_token = get_edl_token(environment)
    print('Retrieved EDL bearer token')

    if file_path is None:
        all_cloud_collections = query_cmr_graph_for_collections(
            environment,
            edl_bearer_token
        )
        save_all_collections(all_cloud_collections, environment)
    else:
        all_cloud_collections = get_all_collections_from_file(file_path)
    print(f'Retrieved {len(all_cloud_collections)} cloud-hosted collections.')

    opendap_collections = filter_for_opendap_granules(
        all_cloud_collections,
        environment
    )
    print(f'{len(opendap_collections)} cloud-hosted collections have OPeNDAP RelatedUrl')

    save_opendap_collections(opendap_collections, environment)

### A bit of hackery


def get_short_names_for_pocloud(data: list[dict]) -> list:
    """
    This function takes a JSON list as input and returns a list of 'short_name' values
    for concepts with 'concept_id' ending in 'POCLOUD'.

    Args:
      data: A JSON list containing dictionaries with 'short_name' and 'concept_id' keys.

    Returns:
      A list of 'short_name' values for concepts with 'concept_id' ending in 'POCLOUD'.
    """
    pocloud_short_names = []
    for item in data:
        if item['concept_id'].endswith('POCLOUD'):
            pocloud_short_names.append(item['short_name'])
    return pocloud_short_names


def read_json_list(filename: str) -> list[dict]:
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {filename} not found.")

    return data


def read_text_file_to_list(filename):
    """
    This function reads a text file with one entry per line and returns a list of those entries.

    Args:
      filename: The name of the text file to read.

    Returns:
      A list of the entries in the text file, with leading/trailing whitespace stripped.
    """
    entries = []
    try:
        with open(filename, 'r') as file:
            for line in file:
                # Strip leading/trailing whitespace from each line
                entries.append(line.strip())
    except FileNotFoundError:
        print(f"Error: File {filename} not found.")
    return entries


def write_list(filename: str, names: list):
    with open(filename, "w") as file:  # Open the file in write mode ("w")
        for element in names:
            file.write(element + "\n")  # Write element with newline character


def find_elements_in_second_list(list1: list, list2: list):
    """
    This function finds elements present in the second list but not in the first,
    and prints them.

    Args:
     list1: The first list to compare.
     list2: The second list to compare.
    """
    # Create a set from the first list for efficient membership checking
    unique_elements = set(list1)
    # Find elements in list2 that are not in the set (and thus not in list1)
    elements_not_in_first = [element for element in list2 if element not in unique_elements]
    if elements_not_in_first:
        print("Elements in the second list not present in the first:")
        for element in elements_not_in_first:
            print(element)
    else:
        print("All elements in the second list are present in the first list.")

