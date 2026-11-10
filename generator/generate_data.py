"""Synthetic retail stock data generator.

Simulates a network of sports stores day by day and writes one landing
partition per day (``dt=YYYY-MM-DD``) with the files an upstream system
would typically drop:

* ``stores.csv``     store referential (first day only)
* ``products.csv``   full product referential extract (first day, then
                     every day the referential changes)
* ``stock.csv``      end-of-day stock snapshot per store x product
* ``sales.csv``      daily sales lines per store x product x channel
* ``receipts.csv``   supplier receipts per store x product

Stock follows a simple replenishment policy (reorder point / order-up-to
level with a supplier lead time), so stockouts appear naturally when demand
spikes, a supplier is late or a product is discontinued.

A few anomalies are injected on purpose so that the data quality layer has
something to catch:

* negative stock values,
* duplicated sales lines (same ``sale_id`` sent twice),
* a missing stock snapshot for one store on one day,
* referential changes over time (price, category, discontinued products).

Usage::

    python generator/generate_data.py --start 2026-09-01 --days 60 --out data/landing
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42

CITIES = [
    ("Lille", "FR", "L"),
    ("Villeneuve-d'Ascq", "FR", "L"),
    ("Paris La Defense", "FR", "L"),
    ("Lyon", "FR", "L"),
    ("Marseille", "FR", "M"),
    ("Bordeaux", "FR", "M"),
    ("Nantes", "FR", "M"),
    ("Toulouse", "FR", "M"),
    ("Strasbourg", "FR", "M"),
    ("Rennes", "FR", "S"),
    ("Grenoble", "FR", "S"),
    ("Montpellier", "FR", "S"),
    ("Bruxelles", "BE", "M"),
    ("Gent", "BE", "S"),
    ("Madrid", "ES", "L"),
    ("Barcelona", "ES", "M"),
    ("Milano", "IT", "M"),
    ("Torino", "IT", "S"),
    ("Casablanca", "MA", "M"),
    ("Rabat", "MA", "S"),
]
STORE_SIZE_FACTOR = {"S": 0.6, "M": 1.0, "L": 1.6}

SPORTS = {
    "Running": ["Chaussures", "Textile", "Montres", "Accessoires"],
    "Randonnee": ["Chaussures", "Sacs a dos", "Tentes", "Textile"],
    "Velo": ["Velos", "Casques", "Pieces", "Accessoires"],
    "Football": ["Ballons", "Chaussures", "Maillots", "Protections"],
    "Natation": ["Maillots", "Lunettes", "Accessoires"],
    "Fitness": ["Halteres", "Tapis", "Textile", "Accessoires"],
    "Tennis": ["Raquettes", "Balles", "Chaussures"],
    "Ski": ["Skis", "Casques", "Textile", "Gants"],
}
BRANDS = ["Nordrun", "Altipeak", "Velocis", "Goalix", "Aqualine", "Fitcore", "Smashpro", "Glacio"]
CHANNELS = ["STORE", "CLICK_COLLECT"]

# Anomaly rates
NEGATIVE_STOCK_RATE = 0.001
DUPLICATE_SALE_RATE = 0.005


@dataclass
class Product:
    product_id: str
    product_name: str
    sport: str
    category: str
    brand: str
    unit_price: float
    status: str
    updated_at: datetime
    popularity: float
    seasonality_peak: int  # day of year where demand peaks


@dataclass
class Store:
    store_id: str
    store_name: str
    city: str
    country: str
    store_size: str
    opening_date: date


@dataclass
class PairState:
    stock: int
    base_demand: float
    pending: list[tuple[date, int, str]] = field(default_factory=list)  # (arrival, qty, supplier)


def poisson(lam: float, rng: random.Random) -> int:
    """Knuth's algorithm, good enough for small lambdas."""
    if lam <= 0:
        return 0
    if lam > 30:  # normal approximation for large lambdas
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    threshold = math.exp(-lam)
    k, p = 0, 1.0
    while True:
        p *= rng.random()
        if p <= threshold:
            return k
        k += 1


def stable_id(*parts: object) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def build_stores(start: date) -> list[Store]:
    stores = []
    for i, (city, country, size) in enumerate(CITIES, start=1):
        stores.append(
            Store(
                store_id=f"S{i:03d}",
                store_name=f"Store {city}",
                city=city,
                country=country,
                store_size=size,
                opening_date=start - timedelta(days=365 * (1 + i % 12)),
            )
        )
    return stores


def build_products(n_products: int, start: date, rng: random.Random) -> list[Product]:
    products = []
    sports = list(SPORTS)
    for i in range(1, n_products + 1):
        sport = sports[i % len(sports)]
        category = rng.choice(SPORTS[sport])
        price = round(rng.choice([4.99, 9.99, 14.99, 19.99, 29.99, 49.99, 79.99, 129.99, 249.99]), 2)
        # cheap items sell more
        popularity = max(0.05, rng.lognormvariate(0, 0.6) * (30 / (price + 10)))
        peak = {"Ski": 15, "Natation": 200, "Velo": 150, "Randonnee": 190}.get(sport, rng.randint(1, 365))
        products.append(
            Product(
                product_id=f"P{i:05d}",
                product_name=f"{sport} {category} {i:05d}",
                sport=sport,
                category=category,
                brand=rng.choice(BRANDS),
                unit_price=price,
                status="ACTIVE",
                updated_at=datetime.combine(start - timedelta(days=30), datetime.min.time()),
                popularity=popularity,
                seasonality_peak=peak,
            )
        )
    return products


def seasonality(day: date, peak: int) -> float:
    doy = day.timetuple().tm_yday
    distance = min(abs(doy - peak), 365 - abs(doy - peak))
    return 0.6 + 0.8 * math.exp(-((distance / 60) ** 2))


def weekday_factor(day: date) -> float:
    return {5: 1.6, 6: 0.8}.get(day.weekday(), 1.0)  # busy Saturday, quieter Sunday


def apply_referential_changes(products: list[Product], day: date, rng: random.Random) -> bool:
    """Randomly change a few products. Returns True if anything changed."""
    changed = False
    ts = datetime.combine(day, datetime.min.time()) + timedelta(hours=6)
    if day.weekday() == 0:  # weekly price review on Mondays
        for p in rng.sample(products, k=max(1, len(products) // 40)):
            if p.status != "ACTIVE":
                continue
            p.unit_price = round(p.unit_price * rng.choice([0.8, 0.9, 1.05, 1.1]), 2)
            p.updated_at = ts
            changed = True
    if rng.random() < 0.15:  # occasional discontinuation
        candidates = [p for p in products if p.status == "ACTIVE"]
        p = rng.choice(candidates)
        p.status = "DISCONTINUED"
        p.updated_at = ts
        changed = True
    if rng.random() < 0.05:  # category re-mapping by the product team
        p = rng.choice(products)
        new_cat = rng.choice(SPORTS[p.sport])
        if new_cat != p.category:
            p.category = new_cat
            p.updated_at = ts
            changed = True
    return changed


def write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def product_rows(products: list[Product]) -> list[list[object]]:
    return [
        [
            p.product_id,
            p.product_name,
            p.sport,
            p.category,
            p.brand,
            f"{p.unit_price:.2f}",
            p.status,
            p.updated_at.isoformat(sep=" "),
        ]
        for p in products
    ]


def generate(start: date, days: int, n_products: int, out: Path) -> None:
    rng = random.Random(SEED)
    stores = build_stores(start)
    products = build_products(n_products, start, rng)
    suppliers = [f"SUP{i:02d}" for i in range(1, 7)]
    late_supplier = "SUP03"  # unreliable supplier: frequent delays

    # Each store carries ~80% of the assortment
    state: dict[tuple[str, str], PairState] = {}
    for s in stores:
        for p in products:
            if rng.random() < 0.8:
                demand = p.popularity * STORE_SIZE_FACTOR[s.store_size]
                state[(s.store_id, p.product_id)] = PairState(
                    stock=max(1, int(demand * rng.uniform(8, 20))), base_demand=demand
                )
    product_by_id = {p.product_id: p for p in products}
    supplier_of = {p.product_id: suppliers[int(p.product_id[1:]) % len(suppliers)] for p in products}

    missing_snapshot = (start + timedelta(days=min(16, days - 1)), "S007")

    for offset in range(days):
        day = start + timedelta(days=offset)
        part = out / f"dt={day.isoformat()}"
        referential_changed = apply_referential_changes(products, day, rng) if offset else False

        if offset == 0:
            write_csv(
                part / "stores.csv",
                ["store_id", "store_name", "city", "country", "store_size", "opening_date"],
                [
                    [s.store_id, s.store_name, s.city, s.country, s.store_size, s.opening_date.isoformat()]
                    for s in stores
                ],
            )
        if offset == 0 or referential_changed:
            write_csv(
                part / "products.csv",
                ["product_id", "product_name", "sport", "category", "brand", "unit_price", "status", "updated_at"],
                product_rows(products),
            )

        stock_rows, sales_rows, receipt_rows = [], [], []
        for (store_id, product_id), st in state.items():
            product = product_by_id[product_id]

            # 1. receipts arriving today
            arrived = [o for o in st.pending if o[0] <= day]
            st.pending = [o for o in st.pending if o[0] > day]
            for _, qty, supplier in arrived:
                st.stock += qty
                receipt_rows.append(
                    [
                        stable_id("R", store_id, product_id, day, qty),
                        day.isoformat(),
                        store_id,
                        product_id,
                        qty,
                        supplier,
                    ]
                )

            # 2. demand, capped by available stock (lost sales on stockout)
            lam = st.base_demand * seasonality(day, product.seasonality_peak) * weekday_factor(day)
            demand = poisson(lam, rng)
            sold = min(demand, st.stock)
            st.stock -= sold
            if sold:
                web = sum(1 for _ in range(sold) if rng.random() < 0.25)
                for channel, qty in (("STORE", sold - web), ("CLICK_COLLECT", web)):
                    if qty:
                        row = [
                            stable_id("S", store_id, product_id, day, channel),
                            day.isoformat(),
                            store_id,
                            product_id,
                            channel,
                            qty,
                            f"{qty * product.unit_price:.2f}",
                        ]
                        sales_rows.append(row)
                        if rng.random() < DUPLICATE_SALE_RATE:
                            sales_rows.append(list(row))  # duplicated line

            # 3. replenishment (no orders for discontinued products)
            reorder_point = st.base_demand * 7
            on_order = sum(o[1] for o in st.pending)
            if product.status == "ACTIVE" and st.stock + on_order <= reorder_point:
                supplier = supplier_of[product_id]
                lead = rng.randint(2, 4)
                if supplier == late_supplier and rng.random() < 0.5:
                    lead += rng.randint(3, 10)  # late delivery
                qty = max(1, int(st.base_demand * 21) - st.stock - on_order)
                # partial deliveries happen
                if rng.random() < 0.1:
                    qty = max(1, qty // 2)
                st.pending.append((day + timedelta(days=lead), qty, supplier))

            # 4. end-of-day snapshot
            if (day, store_id) == missing_snapshot:
                continue  # anomaly: store did not send its snapshot that day
            reported = st.stock
            if rng.random() < NEGATIVE_STOCK_RATE:
                reported = -rng.randint(1, 5)  # anomaly: inventory error upstream
            stock_rows.append([day.isoformat(), store_id, product_id, reported])

        write_csv(part / "stock.csv", ["snapshot_date", "store_id", "product_id", "stock_qty"], stock_rows)
        write_csv(
            part / "sales.csv",
            ["sale_id", "sale_date", "store_id", "product_id", "channel", "quantity", "amount"],
            sales_rows,
        )
        write_csv(
            part / "receipts.csv",
            ["receipt_id", "receipt_date", "store_id", "product_id", "quantity", "supplier_id"],
            receipt_rows,
        )
        print(
            f"{day}: stock={len(stock_rows)} sales={len(sales_rows)} receipts={len(receipt_rows)}"
            f"{' products' if offset == 0 or referential_changed else ''}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 9, 1))
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--products", type=int, default=150)
    parser.add_argument("--out", type=Path, default=Path("data/landing"))
    args = parser.parse_args()
    generate(args.start, args.days, args.products, args.out)


if __name__ == "__main__":
    main()
