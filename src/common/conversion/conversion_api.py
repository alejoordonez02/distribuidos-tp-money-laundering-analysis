from abc import ABC, abstractmethod


class ConversionAPI(ABC):
    @abstractmethod
    def get_rates(self, date: str) -> dict[str, float]:
        """Return a mapping of currency name → USD multiplier for the given date."""
