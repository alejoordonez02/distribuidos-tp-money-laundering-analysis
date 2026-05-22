from abc import ABC, abstractmethod
from datetime import date


class ConversionAPIError(Exception):
    pass


class ConversionAPI(ABC):
    @abstractmethod
    def get_rates(self, day: date) -> dict[str, float]:
        """Return a mapping of currency name → USD multiplier for the given date."""
