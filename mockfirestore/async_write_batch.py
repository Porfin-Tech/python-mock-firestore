from typing import Union

from google.api_core import gapic_v1
from google.api_core import retry as retries

from mockfirestore._helpers import get_by_path
from mockfirestore.async_document import AsyncDocumentReference


class AsyncWriteBatch:
    sets: list[tuple[AsyncDocumentReference, dict, bool]]
    creates: list[tuple[AsyncDocumentReference, dict]]

    def __init__(self):
        self.sets = []
        self.creates = []

    def set(
        self,
        reference: AsyncDocumentReference,
        document_data: dict,
        merge: Union[bool, list] = False,
    ) -> None:
        if isinstance(merge, list):
            raise NotImplementedError()
        self.sets.append((reference, document_data, merge))

    def create(self, reference: AsyncDocumentReference, document_data: dict) -> None:
        self.creates.append((reference, document_data))

    async def commit(
        self, retry: retries.Retry = gapic_v1.method.DEFAULT, timeout: float = None
    ):
        for reference, document_data, merge in self.sets:
            await reference.set(document_data, merge)
        for reference, document_data in self.creates:
            store = get_by_path(reference.parent._data, reference._path)
            if document_data["id"] in store:
                raise NotImplementedError()
            await reference.set(document_data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            await self.commit()
            await self.commit()
