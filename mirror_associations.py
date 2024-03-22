""" An experimental Python module for replicating UMM-C to UMM-S associations
    relating to one service with another. This is designed in support of
    HYRAX-1279.

    The main entrypoint to this module is the `mirror_service_associations`
    function:

    ```
    mirror_service_associations('S2009180097-POCLOUD', 'POCLOUD', 'prod', False)
    ```

    In the call above:

    - 'S2009180097-POCLOUD1' is a UMM-S concept ID of an OPeNDAP UMM-S record
      with collection associations to be replicated.
    - 'POCLOUD' is a provider used to filter down the collections to be
      replicated. Note, the user running this request will need to have
      permissions to create metadata associations in this provider.
    - 'prod' indicates that this should be done in CMR in production. The other
      value that is accepted is 'uat'.
    - False indicates that the original associations between the collections
      and the first UMM-S record should not be removed. Using `True` instead
      would cause the script to attempt to remove these associations.

    The main constraint is that the user running the script has sufficient
    privileges in the collection provider to update associations.

    Gotchas:

    - You'll need to update this script with an active LaunchPad token below.
    - I _think_ creating and deleting multiple associations in a single request
      should work. But I've not tested it. The alternative would be to loop
      through and make request per collection.
    - I've adopted LaunchPad tokens throughout, however, even the association
      endpoint is actually part of the CMR search application, so old scripts
      that had previously been used had adopted EDL bearer tokens instead. I
      think this should work fine, though, and seems the "correct" type of
      authentication to use.

    Owen Littlejohns - 2024-03-21

"""
from typing import List, Literal

import requests


Environment = Literal['uat', 'prod']

CMR_URLS = {'uat': 'https://cmr.uat.earthdata.nasa.gov',
            'prod': 'https://cmr.earthdata.nasa.gov'}
OPENDAP_CONCEPT_IDS = {'uat': 'S1262134530-EEDTEST',
                       'prod': 'S2874702816-XYZ_PROV'}

LAUNCHPAD_TOKEN = '<insert your LaunchPad token here>'


def get_authenticated_session(launchpad_token: str):
    """ Create a `requests.Session` object with an `Authorization` header
        containing a LaunchPad token.

    """
    session = requests.session()
    session.headers.update({'Authorization' : launchpad_token})
    return session


def get_associated_collections(
    authenticated_session: requests.Session,
    base_cmr_url: str,
    umm_s_concept_id: str,
    collection_provider: str
) -> List[str]:
    """ Retrieve a list of UMM-C concept IDs showing all collections from a
        single provider that are associated with the given UMM-S record.

    """
    collection_provider_lower = collection_provider.lower()

    search_response = authenticated_session.get(
        f'{base_cmr_url}/search/services.umm_json',
        params={'concept_id': umm_s_concept_id}
    )
    search_response.raise_for_status()

    try:
        service_json = search_response.json().get('items')[0]
    except IndexError:
        raise f'Could not find service record: {umm_s_concept_id}' from IndexError

    collections_in_provider = [
        collection
        for collection
        in service_json.get('meta').get('associations', {}).get('collections', [])
        if collection.lower().endswith(collection_provider_lower)
    ]

    return collections_in_provider


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


def remove_associations(
    authenticated_session: requests.Session,
    base_cmr_url: str,
    umm_s_concept_id: str,
    collection_concept_ids: List[str]
):
    """ Make a single request to CMR to remove associations between the
        specified UMM-S record and all listed UMM-C records.

        This is almost identical to `create_associations`, just using a
        different HTTP request method.

    """
    json_payload = [{'concept_id': collection_concept_id}
                    for collection_concept_id in collection_concept_ids]

    delete_response = authenticated_session.delete(
        f'{base_cmr_url}/search/services/{umm_s_concept_id}/associations',
        json=json_payload
    )
    delete_response.raise_for_status()


def mirror_service_associations(service_with_associations: str,
                                collection_provider: str,
                                environment: Environment,
                                remove_old_associations: bool):
    """ Query CMR for all the collections associated with one collections,
        then replicate those associations with the official OPeNDAP UMM-S
        record in that environment (production or UAT). Optionally, remove the
        associations between the collections and the first service record.

        service_with_associations - UMM-S concept ID.
        collection_provider - string to filter collections being replicated.
        environment - 'uat' or 'prod'
        remove_associations - True or False. If True, the associations between
          service_with_associations and all collections in collection_provider.

    """
    authenticated_session = get_authenticated_session(LAUNCHPAD_TOKEN)
    base_cmr_url = CMR_URLS.get(environment)
    opendap_concept_id = OPENDAP_CONCEPT_IDS.get(environment)

    if base_cmr_url is None or opendap_concept_id is None:
        raise Exception(f'Invalid environment: {environment}')

    associated_collections = get_associated_collections(
        authenticated_session, base_cmr_url, service_with_associations,
        collection_provider
    )

    create_associations(
        authenticated_session, base_cmr_url, opendap_concept_id,
        associated_collections
    )

    if remove_old_associations:
        remove_associations(
            authenticated_session, base_cmr_url, service_with_associations,
            associated_collections
        )
