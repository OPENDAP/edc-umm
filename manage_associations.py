""" An experimental Python module for associating all collections from a
    specified provider with the enterprise OPeNDAP UMM-S record. This is
    designed in support of HYRAX-1279 (and TRT-411, TRT-368).

    The main entrypoint to this module is the `make_opendap_associations`
    function:

    ```
    make_opendap_associations('POCLOUD', 'prod')
    ```

    In the call above:

    - 'POCLOUD' is a provider used to filter down the full list of collections
      to be associated. This list was determined based on the first granule in
      the collection having a RelatedUrl with type "USE SERVICE API", a
      subtype of "OPENDAP DATA" and a URL with "opendap.earthdata.nasa.gov", or
      "opendap.uat.earthdata.nasa.gov" for UAT. Note, the user running this
      request will need to have permissions to create metadata associations in
      this provider.
    - 'prod' indicates that this should be done in CMR in production. The other
      value that is accepted is 'uat'.

    Gotchas:

    - You'll need to update this script with an active LaunchPad token below.
    - I've adopted LaunchPad tokens throughout, however, even the association
      endpoint is actually part of the CMR search application, so old scripts
      that had previously been used had adopted EDL bearer tokens instead. I
      think this should work fine, though, and seems the "correct" type of
      authentication to use.
    - You'll need the `opendap_collections_uat.json` and/or
      `opendaop_collections_prod.json` in the same directory as this module to
      successfully run it.

    Owen Littlejohns - 2024-03-21

"""
from typing import List, Literal
import json

import requests


Environment = Literal['uat', 'prod']

CMR_URLS = {'uat': 'https://cmr.uat.earthdata.nasa.gov',
            'prod': 'https://cmr.earthdata.nasa.gov'}

OPENDAP_CONCEPT_IDS = {'uat': 'S1262134530-EEDTEST',
                       'prod': 'S2874702816-XYZ_PROV'}

LAUNCHPAD_TOKEN = '<Insert Launchpad token here>'


def get_authenticated_session(launchpad_token: str):
    """ Create a `requests.Session` object with an `Authorization` header
        containing a LaunchPad token.

    """
    session = requests.session()
    session.headers.update({'Authorization' : launchpad_token})
    return session


def read_all_collections(environment: Environment) -> list[dict]:
    """ Parse the correct JSON file to retrieve all collections where the first
        granule has OPeNDAP metadata.

        This file assumes that `opendap_collection_<environment>.json` is a
        sibling file to this script.

    """
    json_file_name = f'opendap_collections_{environment}.json'

    with open(json_file_name, encoding='utf-8') as file_handler:
        all_collections = json.load(file_handler)

    return all_collections


def get_provider_collections(
    collection_provider: str,
    environment: Environment
) -> List[str]:
    """ Retrieve a list of UMM-C concept IDs showing all collections from a
        single provider.

    """
    collection_provider_lower = collection_provider.lower()
    all_collections = read_all_collections(environment)

    return [
        collection['concept_id']
        for collection
        in all_collections
        if collection.get('concept_id', '').lower().endswith(collection_provider_lower)
    ]


def create_associations(
    authenticated_session: requests.Session,
    base_cmr_url: str,
    umm_s_concept_id: str,
    collection_concept_ids: List[str]
):
    """ Make a single request to CMR to create associations between the
        specified UMM-S record and all listed UMM-C records.

    """
    json_payload = [{'concept_id': collection_concept_id}
                    for collection_concept_id in collection_concept_ids]

    create_response = authenticated_session.post(
        f'{base_cmr_url}/search/services/{umm_s_concept_id}/associations',
        json=json_payload
    )
    create_response.raise_for_status()


def make_opendap_associations(
    collection_provider: str,
    environment: Environment,
):
    """ Filter a list of collections that are known to have OPeNDAP-capable
        URLs (Hyrax, not on-premises), and associate those collections with
        the enterprise UMM-S record for Hyrax.

        collection_provider - string to filter collections being replicated.
        environment - 'uat' or 'prod'

    """
    authenticated_session = get_authenticated_session(LAUNCHPAD_TOKEN)
    base_cmr_url = CMR_URLS.get(environment)
    opendap_concept_id = OPENDAP_CONCEPT_IDS.get(environment)

    if base_cmr_url is None or opendap_concept_id is None:
        raise Exception(f'Invalid environment: {environment}')

    provider_collections = get_provider_collections(
        collection_provider,
        environment
    )
    print(f'Found {len(provider_collections)} for {collection_provider}')

    create_associations(
        authenticated_session,
        base_cmr_url,
        opendap_concept_id,
        provider_collections
    )
    print('Made associations')
