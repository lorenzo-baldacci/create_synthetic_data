import random
import re
from typing import Callable, List, Tuple

import pandas as pd
from faker import Faker

_faker = Faker()

# (pattern, generator_factory) — evaluated in order; first match wins
_NAME_PATTERNS: List[Tuple[str, Callable]] = [
    (r"\bemail\b", lambda f: f.email),
    (r"first_?name", lambda f: f.first_name),
    (r"last_?name", lambda f: f.last_name),
    (r"full_?name|^name$", lambda f: f.name),
    (r"phone|mobile", lambda f: f.phone_number),
    (r"\baddress\b", lambda f: f.address),
    (r"\bcity\b", lambda f: f.city),
    (r"\bcountry\b", lambda f: f.country),
    (r"zip|postal|postcode", lambda f: f.postcode),
    (r"company|employer", lambda f: f.company),
    (r"url|website", lambda f: f.url),
    (r"description|^text$|notes|comment", lambda f: lambda: f.text(max_nb_chars=200)),
    (r"created_at|updated_at|birth|^date", lambda f: lambda: f.date_between(start_date="-2y", end_date="today")),
    (r"timestamp|^ts$", lambda f: lambda: f.date_time_between(start_date="-2y", end_date="now")),
    (r"price|amount|cost|salary", lambda f: lambda: round(random.uniform(1, 10000), 2)),
    (r"\bage\b", lambda f: lambda: random.randint(18, 90)),
    (r"\bstatus\b", lambda f: lambda: random.choice(["active", "inactive", "pending"])),
    (r"\bgender\b", lambda f: lambda: random.choice(["M", "F", "Other"])),
    # id handled separately below (type-aware: uuid4 for STRING, int for numeric)
]

_INT_TYPES = {"INT", "INTEGER", "BIGINT", "LONG"}

_TYPE_FALLBACKS = {
    "INT": lambda: random.randint(1, 1_000_000),
    "INTEGER": lambda: random.randint(1, 1_000_000),
    "BIGINT": lambda: random.randint(1, 1_000_000),
    "LONG": lambda: random.randint(1, 1_000_000),
    "FLOAT": lambda: round(random.uniform(0, 10000), 4),
    "DOUBLE": lambda: round(random.uniform(0, 10000), 4),
    "BOOLEAN": lambda: random.choice([True, False]),
    "BOOL": lambda: random.choice([True, False]),
    "DATE": lambda: _faker.date_between(start_date="-2y", end_date="today"),
    "TIMESTAMP": lambda: _faker.date_time_between(start_date="-2y", end_date="now"),
}


def get_column_generator(col_name: str, col_type: str, faker_instance: Faker) -> Callable:
    """Return a zero-argument callable that produces one value for the given column."""
    lower = col_name.lower()
    type_upper = col_type.upper()

    # id columns: use int for numeric types, uuid4 for string types
    if re.search(r"(^|_)id$|\bid\b", lower):
        if type_upper in _INT_TYPES:
            return lambda: random.randint(1, 1_000_000)
        return faker_instance.uuid4

    for pattern, factory in _NAME_PATTERNS:
        if re.search(pattern, lower):
            return factory(faker_instance)

    # Type fallback
    return _TYPE_FALLBACKS.get(type_upper, faker_instance.word)


def generate_dataframe(columns: List[Tuple[str, str]], num_rows: int) -> pd.DataFrame:
    """Generate a pandas DataFrame with num_rows of synthetic data."""
    f = Faker()
    generators = [(name, get_column_generator(name, sql_type, f)) for name, sql_type in columns]

    rows = []
    for _ in range(num_rows):
        row = {name: gen() for name, gen in generators}
        rows.append(row)

    return pd.DataFrame(rows)
