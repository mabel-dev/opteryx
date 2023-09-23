from typing import Iterable
from typing import Optional

from opteryx import config
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError
from opteryx.managers.kvstores import BaseKeyValueStore

GCP_PROJECT_ID = config.GCP_PROJECT_ID


def _get_project_id():  # pragma: no cover
    """Fetch the ID from GCP"""
    try:
        import requests
    except ImportError as exception:  # pragma: no cover
        raise UnmetRequirementError(
            "Firestore requires 'GCP_PROJECT_ID` to be set in config, or "
            "`requests` to be installed."
        ) from exception

    # if it's set in the config/environ, use that
    if GCP_PROJECT_ID:
        return GCP_PROJECT_ID

    # otherwise try to get it from GCP
    response = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/project/project-id",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    response.raise_for_status()
    return response.text


def _initialize():  # pragma: no cover
    """Create the connection to Firebase"""
    try:
        import firebase_admin
        from firebase_admin import credentials
    except ImportError as err:  # pragma: no cover
        raise MissingDependencyError(err.name) from err
    if not firebase_admin._apps:
        # if we've not been given the ID, fetch it
        project_id = GCP_PROJECT_ID
        if project_id is None:
            project_id = _get_project_id()
        creds = credentials.ApplicationDefault()
        firebase_admin.initialize_app(creds, {"projectId": project_id})


class FireStoreKVStore(BaseKeyValueStore):
    def get(self, key: bytes) -> Optional[bytes]:
        from firebase_admin import firestore

        _initialize()
        database = firestore.client()
        document = database.collection(self._location).document(key).get()
        if document.exists:
            return document.to_dict()
        return None

    def set(self, key: bytes, value: bytes):
        _initialize()
        from firebase_admin import firestore

        database = firestore.client()
        database.collection(self._location).document(key).set(value)
        return True

    def contains(self, keys: Iterable) -> Iterable:
        _initialize()
        from firebase_admin import firestore

        database = firestore.client()
        collection = database.collection(self._location)
        found = []
        for key in keys:
            doc = collection.document(key).get()
            if doc.exists:
                found.append(key)
        return found
