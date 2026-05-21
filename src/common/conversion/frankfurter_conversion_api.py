from datetime import date

import requests

from .conversion_api import ConversionAPI, ConversionAPIError

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
_TIMEOUT = 10
_JSON_RATES_KEY = "rates"
_JSON_USD_KEY = "USD"
_TARGET_CURRENCIES = {"USD", "EUR"}


class FrankfurterConversionAPI(ConversionAPI):
    def get_rates(self, day: date) -> dict[str, float]:
        try:
            resp = requests.get(_FRANKFURTER_URL.format(date=day.isoformat()), timeout=_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise ConversionAPIError(str(e)) from e

        eur_rates = resp.json()[_JSON_RATES_KEY]
        usd_per_eur = eur_rates[_JSON_USD_KEY]

        rates: dict[str, float] = {"US Dollar": 1.0, "Euro": usd_per_eur}
        for name, iso in _CURRENCY_ISO.items():
            if iso in _TARGET_CURRENCIES:
                continue
            if iso in eur_rates:
                rates[name] = usd_per_eur / eur_rates[iso]

        rates.update(_USD_FALLBACK)
        return rates
