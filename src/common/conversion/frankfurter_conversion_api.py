import requests

from .conversion_api import ConversionAPI

_CURRENCY_ISO = {
    "Australian Dollar": "AUD",
    "Brazil Real": "BRL",
    "Canadian Dollar": "CAD",
    "Euro": "EUR",
    "Mexican Peso": "MXN",
    "Rupee": "INR",
    "Shekel": "ILS",
    "Swiss Franc": "CHF",
    "UK Pound": "GBP",
    "US Dollar": "USD",
    "Yen": "JPY",
    "Yuan": "CNY",
}

_USD_FALLBACK = {
    "Bitcoin": 78.33,
    "Ruble": 0.01,
    "Saudi Riyal": 0.27,
}

_FRANKFURTER_URL = "https://api.frankfurter.app/{date}"


class FrankfurterConversionAPI(ConversionAPI):
    def get_rates(self, date: str) -> dict[str, float]:
        resp = requests.get(_FRANKFURTER_URL.format(date=date), timeout=10)
        resp.raise_for_status()
        eur_rates = resp.json()["rates"]
        usd_per_eur = eur_rates["USD"]

        rates: dict[str, float] = {"US Dollar": 1.0, "Euro": usd_per_eur}
        for name, iso in _CURRENCY_ISO.items():
            if iso in ("USD", "EUR"):
                continue
            if iso in eur_rates:
                rates[name] = usd_per_eur / eur_rates[iso]

        rates.update(_USD_FALLBACK)
        return rates
