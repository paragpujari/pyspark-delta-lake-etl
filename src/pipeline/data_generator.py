import csv
import random
from datetime import date, timedelta
from pathlib import Path


def generate_source_data(output_dir: Path, seed: int = 42) -> None:
    """Create deterministic sample dimensions and orders, including bad rows/duplicates."""
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    customers = [
        {"customer_id": f"C{i:03d}", "customer_name": name, "city": city, "segment": segment}
        for i, (name, city, segment) in enumerate([
            ("Aarav Sharma", "Hyderabad", "Consumer"),
            ("Diya Patel", "Mumbai", "Corporate"),
            ("Kabir Reddy", "Bengaluru", "Small Business"),
            ("Meera Das", "Bhubaneswar", "Consumer"),
            ("Ishaan Rao", "Chennai", "Corporate"),
            ("Anaya Singh", "Pune", "Consumer"),
            ("Vivaan Nair", "Kochi", "Small Business"),
            ("Saanvi Gupta", "Delhi", "Corporate"),
        ], start=1)
    ]
    products = [
        {"product_id": "P001", "product_name": "Laptop", "category": "Electronics"},
        {"product_id": "P002", "product_name": "Headphones", "category": "Electronics"},
        {"product_id": "P003", "product_name": "Office Chair", "category": "Furniture"},
        {"product_id": "P004", "product_name": "Desk", "category": "Furniture"},
        {"product_id": "P005", "product_name": "Notebook Pack", "category": "Stationery"},
        {"product_id": "P006", "product_name": "Monitor", "category": "Electronics"},
    ]

    def write_csv(filename, rows):
        with (output_dir / filename).open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    write_csv("customers.csv", customers)
    write_csv("products.csv", products)

    orders = []
    start = date(2025, 1, 1)
    statuses = ["Completed", "Shipped", "Pending", "Cancelled"]
    regions = ["South", "West", "North", "East"]
    for i in range(1, 151):
        orders.append({
            "order_id": f"O{i:04d}",
            "customer_id": rng.choice(customers)["customer_id"],
            "product_id": rng.choice(products)["product_id"],
            "order_date": (start + timedelta(days=rng.randint(0, 179))).isoformat(),
            "quantity": str(rng.randint(1, 5)),
            "unit_price": str(rng.choice([499, 799, 1299, 2499, 4999, 69999, 89999])),
            "discount": str(rng.choice([0, 0.05, 0.10, 0.15])),
            "status": rng.choice(statuses),
            "region": rng.choice(regions),
        })

    # Deliberate data-quality cases: missing key, invalid date, bad quantity, invalid discount.
    orders.extend([
        {"order_id": "O_BAD01", "customer_id": "C001", "product_id": "P001", "order_date": "2025-02-30", "quantity": "1", "unit_price": "100", "discount": "0", "status": "Completed", "region": "South"},
        {"order_id": "O_BAD02", "customer_id": "C002", "product_id": "P002", "order_date": "2025-02-10", "quantity": "0", "unit_price": "100", "discount": "0", "status": "Pending", "region": "West"},
        {"order_id": "O_BAD03", "customer_id": "C003", "product_id": "P003", "order_date": "2025-02-11", "quantity": "2", "unit_price": "100", "discount": "1.5", "status": "Shipped", "region": "East"},
        {"order_id": "", "customer_id": "C004", "product_id": "P004", "order_date": "2025-02-12", "quantity": "1", "unit_price": "100", "discount": "0", "status": "Completed", "region": "North"},
    ])
    # Duplicate business key; the pipeline keeps the latest ingested occurrence.
    duplicate = dict(orders[0])
    duplicate["quantity"] = "2"
    orders.append(duplicate)
    write_csv("orders.csv", orders)
