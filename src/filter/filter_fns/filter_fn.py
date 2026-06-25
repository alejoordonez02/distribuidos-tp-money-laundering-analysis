from abc import abstractmethod

from common.comms.messages import Message, Transactions


class FilterFn:
    def filter(self, el: Message) -> Message:
        """
        Filter an element.

        Template method: keep every transaction for which ``_keep`` returns
        True (preserving order) and return a new ``Transactions`` carrying the
        same ``client_id``. Simple per-transaction filters only need to define
        ``_keep``.

        Filters whose output is not a per-transaction ``Transactions`` projection
        (e.g. ``Promiscuous``, ``UC3AvgFilter``, ``UC4PathFilter``) override this
        method directly instead.

        Returns the filtered element.
        """
        filtered = [t for t in el.transactions if self._keep(t)]
        return Transactions(el.client_id, filtered)

    @abstractmethod
    def _keep(self, t) -> bool:
        """Return True if the transaction must be kept (override in simple filters)."""
        raise NotImplementedError
