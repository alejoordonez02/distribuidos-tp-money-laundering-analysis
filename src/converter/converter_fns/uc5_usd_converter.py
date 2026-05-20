import json
import urllib.request
from dataclasses import replace

from common.comms.messages import Transactions

from .converter_fn import ConverterFn

# Full currency name → ISO 4217 code (Frankfurter API uses ISO codes)
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

# Static fallback rates (→ USD) for currencies absent from the Frankfurter/ECB dataset
_USD_FALLBACK = {
    "Bitcoin": 78.33,
    "Ruble": 0.01,      # RUB suspended by ECB in early 2022
    "Saudi Riyal": 0.27, # SAR not in ECB reference rates
}

_FRANKFURTER_URL = "https://api.frankfurter.app/{date}"


class UC5USDConverterFn(ConverterFn):
    def __init__(self):
        # date string (YYYY-MM-DD) → {currency_name: usd_rate}
        self._cache: dict[str, dict[str, float]] = {}

    def convert(self, msg: Transactions) -> Transactions:
        dates = {str(t.timestamp)[:10] for t in msg.transactions}
        for date in dates:
            if date not in self._cache:
                self._cache[date] = self._fetch_rates(date)

        converted = [
            replace(
                t,
                amount_paid=t.amount_paid * self._cache[str(t.timestamp)[:10]].get(t.payment_currency, 1.0),
                payment_currency="US Dollar",
            )
            for t in msg.transactions
        ]
        return Transactions(msg.client_id, converted)

    def _fetch_rates(self, date: str) -> dict[str, float]:
        url = _FRANKFURTER_URL.format(date=date)
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())

        # Response: {base: "EUR", rates: {ISO: units_per_EUR, ...}}
        # To get USD per unit of currency X: usd_per_eur / eur_per_x
        eur_rates = data["rates"]
        usd_per_eur = eur_rates["USD"]

        rates: dict[str, float] = {"US Dollar": 1.0, "Euro": usd_per_eur}
        for name, iso in _CURRENCY_ISO.items():
            if iso in ("USD", "EUR"):
                continue
            if iso in eur_rates:
                rates[name] = usd_per_eur / eur_rates[iso]

        rates.update(_USD_FALLBACK)
        return rates
