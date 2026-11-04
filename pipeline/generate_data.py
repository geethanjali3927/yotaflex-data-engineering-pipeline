"""Generate deterministic synthetic customer and order CSV input files."""

import argparse
import csv
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
import random
from typing import Sequence


CUSTOMER_HEADERS = (
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "signup_date",
    "country_code",
    "status",
    "phone",
)
ORDER_HEADERS = (
    "order_id",
    "customer_id",
    "order_ts",
    "total_amount_cents",
    "currency_code",
    "status",
)

_FIRST_NAMES = (
    "Alex",
    "Amina",
    "Amir",
    "Anika",
    "Chen",
    "Daniel",
    "Elena",
    "Fatima",
    "Grace",
    "Hana",
    "Isabel",
    "James",
    "Jordan",
    "Kai",
    "Liam",
    "Maya",
    "Noah",
    "Priya",
    "Sam",
    "Sofia",
)
_LAST_NAMES = (
    "Ahmed",
    "Brown",
    "Chen",
    "Garcia",
    "Hernandez",
    "Johnson",
    "Khan",
    "Kim",
    "Lee",
    "Martin",
    "Miller",
    "Patel",
    "Robinson",
    "Singh",
    "Smith",
    "Taylor",
    "Wang",
    "Wilson",
)
_COUNTRY_CODES = (
    "AU",
    "BR",
    "CA",
    "DE",
    "ES",
    "FR",
    "GB",
    "IN",
    "JP",
    "MX",
    "NL",
    "NZ",
    "SG",
    "US",
)
_ORDER_STATUSES = (
    "placed",
    "paid",
    "shipped",
    "delivered",
    "cancelled",
    "returned",
)
_GENERATION_END_DATE = date(2026, 11, 4)


def generate_data(
    customers: int = 1000,
    orders: int = 5000,
    output_dir: str | Path = Path("data") / "sample",
    seed: int = 42,
) -> tuple[Path, Path]:
    """Write reproducible customers.csv and orders.csv and return their paths."""
    if customers < 0:
        raise ValueError("customers must be non-negative")
    if orders < 0:
        raise ValueError("orders must be non-negative")
    if orders > 0 and customers == 0:
        raise ValueError("orders cannot be generated without customers")

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    customers_path = destination / "customers.csv"
    orders_path = destination / "orders.csv"
    rng = random.Random(seed)

    customer_records: list[tuple[str, str]] = []
    with customers_path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(CUSTOMER_HEADERS)
        for index in range(1, customers + 1):
            customer_id = f"C{index:06d}"
            first_name = rng.choice(_FIRST_NAMES)
            last_name = rng.choice(_LAST_NAMES)
            email = f"{first_name.lower()}.{last_name.lower()}.{index}@example.com"
            signup_date = _random_date(
                rng,
                date(2018, 1, 1),
                _GENERATION_END_DATE - timedelta(days=1),
            )
            country_code = rng.choice(_COUNTRY_CODES)
            status = rng.choices(
                ("active", "inactive"),
                weights=(9, 1),
                k=1,
            )[0]
            phone = _random_phone(rng)
            writer.writerow(
                (
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    signup_date.isoformat(),
                    country_code,
                    status,
                    phone,
                )
            )
            customer_records.append((customer_id, signup_date.isoformat()))

    with orders_path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(ORDER_HEADERS)
        for index in range(1, orders + 1):
            customer_id, signup_date_text = rng.choice(customer_records)
            signup_date = date.fromisoformat(signup_date_text)
            order_date = _random_date(
                rng,
                signup_date,
                _GENERATION_END_DATE,
            )
            order_time = time(
                hour=rng.randrange(24),
                minute=rng.randrange(60),
                second=rng.randrange(60),
                tzinfo=timezone.utc,
            )
            order_timestamp = datetime.combine(
                order_date,
                order_time,
            ).isoformat(timespec="seconds")
            writer.writerow(
                (
                    f"O{index:07d}",
                    customer_id,
                    order_timestamp,
                    rng.randint(199, 250_000),
                    "USD",
                    rng.choice(_ORDER_STATUSES),
                )
            )

    print(f"Customers: {customers_path} ({customers} records)")
    print(f"Orders: {orders_path} ({orders} records)")
    return customers_path, orders_path


def main(argv: Sequence[str] | None = None) -> int:
    """Generate synthetic CSV files from command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic customer and order CSVs."
    )
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--orders", type=int, default=5000)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data") / "sample",
    )
    parser.add_argument("--seed", type=int, default=42)
    arguments = parser.parse_args(argv)

    try:
        generate_data(
            customers=arguments.customers,
            orders=arguments.orders,
            output_dir=arguments.output_dir,
            seed=arguments.seed,
        )
    except ValueError as error:
        parser.error(str(error))
    return 0


def _random_date(
    rng: random.Random,
    start: date,
    end: date,
) -> date:
    """Choose a uniformly distributed date from the inclusive range."""
    day_count = (end - start).days
    return start + timedelta(days=rng.randrange(day_count + 1))


def _random_phone(rng: random.Random) -> str:
    digits = "".join(str(rng.randrange(10)) for _ in range(10))
    if rng.random() < 0.35:
        return f"+1{digits}"
    return f"0{digits}"


if __name__ == "__main__":
    raise SystemExit(main())
