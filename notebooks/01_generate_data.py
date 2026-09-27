# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate the raw data in the landing volume
# MAGIC
# MAGIC *Sahra Retail Lakehouse · by Imran Sheikh · [LinkedIn](https://www.linkedin.com/in/imranazhar/)*
# MAGIC
# MAGIC Alternative to uploading the `data/` folder by hand. Run all cells: the files are written straight into
# MAGIC `/Volumes/workspace/sahra_bronze/landing/…`. Takes under a minute. Seeded, so you get the same data as the download.
# MAGIC
# MAGIC ```
# MAGIC Sahra Retail - synthetic data generator
# MAGIC =======================================
# MAGIC Creates a fictional UAE retail chain's raw data for the lakehouse project:
# MAGIC
# MAGIC   data/stores/stores.csv                  12 stores across the 7 emirates + online
# MAGIC   data/products/products.csv              110 products in 6 categories (prices in AED)
# MAGIC   data/customers/customers_2025-01-01.csv first customer snapshot (CRM export)
# MAGIC   data/customers/customers_2025-07-01.csv mid-year CRM export: changed + H1 sign-ups (for SCD Type 2)
# MAGIC   data/customers/customers_2025-12-31.csv year-end CRM export: H2 sign-ups
# MAGIC   data/orders/orders_2025_MM.json         12 monthly files, JSON Lines, nested order items
# MAGIC
# MAGIC The data is deliberately imperfect so the Silver layer has real work to do:
# MAGIC   - duplicate orders (the same order sent twice by the POS)
# MAGIC   - customers who change loyalty tier or emirate mid-year (history must be kept)
# MAGIC   - lower-case / padded store ids ("st03 ")
# MAGIC   - mixed-case status values ("completed", "Completed ")
# MAGIC   - a second timestamp format ("dd/MM/yyyy HH:mm")
# MAGIC   - zero / negative quantities and missing prices on some items
# MAGIC   - orders pointing at a customer id that does not exist
# MAGIC
# MAGIC Seasonality follows the UAE calendar: Saturday/Sunday weekend, Ramadan (Mar 2025),
# MAGIC Eid al-Fitr and Eid al-Adha, summer slowdown, back-to-school, White Friday and the
# MAGIC Dubai Shopping Festival in December/January.
# MAGIC
# MAGIC Run:  python generate_data.py            (writes to ../data)
# MAGIC Everything is seeded, so every run produces the same files.
# MAGIC ```

# COMMAND ----------

import csv, json, math, os, random
from datetime import date, datetime, timedelta

SEED = 42
random.seed(SEED)
OUT = "/Volumes/workspace/sahra_bronze/landing"   # writes straight into the landing volume
YEAR = 2025

# --------------------------------------------------------------------------- stores
STORES = [
    # id, name, city, emirate, format, lat, lon, opened, size_sqm, traffic weight
    ("ST01", "Sahra Downtown Dubai",   "Dubai",          "Dubai",          "Hypermarket", 25.1972, 55.2744, "2016-03-12", 9500, 1.45),
    ("ST02", "Sahra Deira",            "Dubai",          "Dubai",          "Hypermarket", 25.2517, 55.3325, "2014-09-01", 8800, 1.25),
    ("ST03", "Sahra Express JLT",      "Dubai",          "Dubai",          "Express",     25.0693, 55.1413, "2019-11-20", 900,  0.55),
    ("ST04", "Sahra Yas Island",       "Abu Dhabi",      "Abu Dhabi",      "Hypermarket", 24.4886, 54.6073, "2017-06-18", 9100, 1.30),
    ("ST05", "Sahra Al Wahda",         "Abu Dhabi",      "Abu Dhabi",      "Hypermarket", 24.4700, 54.3727, "2015-02-10", 7600, 1.05),
    ("ST06", "Sahra Express Khalifa City", "Abu Dhabi",  "Abu Dhabi",      "Express",     24.4190, 54.5780, "2021-04-04", 850,  0.45),
    ("ST07", "Sahra Al Nahda Sharjah", "Sharjah",        "Sharjah",        "Hypermarket", 25.3006, 55.3727, "2018-01-15", 7200, 1.00),
    ("ST08", "Sahra Al Ain Central",   "Al Ain",         "Abu Dhabi",      "Hypermarket", 24.2075, 55.7447, "2018-10-07", 6400, 0.70),
    ("ST09", "Sahra Express Al Hamra", "Ras Al Khaimah", "Ras Al Khaimah", "Express",     25.6960, 55.7810, "2022-02-22", 780,  0.35),
    ("ST10", "Sahra Express Ajman",    "Ajman",          "Ajman",          "Express",     25.4052, 55.4430, "2020-08-30", 820,  0.40),
    ("ST11", "Sahra Fujairah City",    "Fujairah",       "Fujairah",       "Express",     25.1288, 56.3265, "2023-05-14", 760,  0.30),
    ("ST12", "Sahra Online",           "Online",         "Online",         "Online",      None,    None,    "2020-04-01", 0,    1.20),
]

# --------------------------------------------------------------------------- products
# category -> list of (name, subcategory, price range AED, cost ratio)
CATALOG = {
    "Grocery": [
        ("Basmati Rice 5kg", "Staples", (38, 55)), ("Medjool Dates 1kg", "Dates & Nuts", (45, 90)),
        ("Khalas Dates 500g", "Dates & Nuts", (18, 32)), ("Arabic Coffee 250g", "Coffee & Tea", (22, 40)),
        ("Saffron 2g", "Spices", (25, 45)), ("Za'atar Mix 200g", "Spices", (9, 16)),
        ("Extra Virgin Olive Oil 1L", "Staples", (32, 48)), ("Labneh 500g", "Dairy", (11, 17)),
        ("Camel Milk 1L", "Dairy", (14, 20)), ("Halloumi 250g", "Dairy", (15, 22)),
        ("Hummus 400g", "Chilled", (8, 13)), ("Arabic Bread 6pc", "Bakery", (3, 6)),
        ("Mixed Nuts 500g", "Dates & Nuts", (35, 60)), ("Tahini 450g", "Staples", (12, 19)),
        ("Chicken Breast 1kg", "Fresh Meat", (28, 38)), ("Lamb Chops 1kg", "Fresh Meat", (65, 90)),
        ("Hammour Fillet 1kg", "Seafood", (70, 110)), ("Tomatoes 1kg", "Fresh Produce", (5, 9)),
        ("Bananas 1kg", "Fresh Produce", (6, 9)), ("Mangoes 1kg", "Fresh Produce", (12, 22)),
        ("Pasta 500g", "Staples", (5, 9)), ("Honey Sidr 500g", "Staples", (80, 160)),
        ("Oats 1kg", "Breakfast", (12, 18)), ("Eggs 30pc", "Dairy", (22, 30)),
        ("Vermicelli 400g", "Staples", (4, 7)),
    ],
    "Beverages": [
        ("Laban 1L", "Dairy Drinks", (5, 8)), ("Water 12x500ml", "Water", (8, 13)),
        ("Karak Tea Mix 20pc", "Hot Drinks", (14, 22)), ("Orange Juice 1.5L", "Juices", (9, 15)),
        ("Vimto Cordial 710ml", "Cordials", (10, 14)), ("Sparkling Water 6pk", "Water", (12, 18)),
        ("Energy Drink 4pk", "Soft Drinks", (18, 26)), ("Cola 6x330ml", "Soft Drinks", (11, 16)),
        ("Green Tea 100pc", "Hot Drinks", (16, 28)), ("Coconut Water 1L", "Juices", (10, 16)),
        ("Iced Coffee 4pk", "Soft Drinks", (20, 28)), ("Mint Lemonade 1L", "Juices", (8, 12)),
        ("Rooh Afza 800ml", "Cordials", (12, 18)), ("Protein Shake 330ml", "Soft Drinks", (9, 14)),
        ("Turkish Coffee 250g", "Hot Drinks", (18, 30)),
    ],
    "Electronics": [
        ("Wireless Earbuds", "Audio", (149, 699)), ("Power Bank 20000mAh", "Accessories", (79, 199)),
        ("Smart Watch", "Wearables", (399, 1599)), ("55-inch 4K Smart TV", "TV", (1499, 3299)),
        ("14-inch Laptop", "Computing", (2199, 5499)), ("Smartphone 128GB", "Mobile", (999, 3999)),
        ("Bluetooth Speaker", "Audio", (129, 599)), ("USB-C Charger 65W", "Accessories", (69, 179)),
        ("Gaming Console", "Gaming", (1799, 2299)), ("Tablet 11-inch", "Computing", (1299, 3499)),
        ("Air Fryer 5L", "Small Appliances", (249, 599)), ("Robot Vacuum", "Small Appliances", (699, 2199)),
        ("Espresso Machine", "Small Appliances", (499, 1899)), ("Noise Cancelling Headphones", "Audio", (599, 1499)),
        ("Portable Fan", "Small Appliances", (49, 129)), ("Action Camera", "Cameras", (899, 1899)),
        ("Wi-Fi Router", "Networking", (199, 699)), ("Hair Dryer", "Personal Care", (149, 999)),
        ("Electric Kettle", "Small Appliances", (79, 249)), ("Phone Case", "Accessories", (29, 119)),
    ],
    "Home": [
        ("Bakhoor Burner", "Fragrance", (45, 180)), ("Prayer Mat", "Living", (35, 120)),
        ("Bedsheet Set King", "Bedding", (129, 399)), ("Bath Towel Set", "Bath", (79, 229)),
        ("Non-stick Pan 28cm", "Kitchen", (69, 189)), ("Dinner Set 24pc", "Kitchen", (149, 499)),
        ("Dallah Coffee Pot", "Kitchen", (59, 249)), ("Floor Cushion Majlis", "Living", (99, 349)),
        ("Blackout Curtains", "Living", (119, 329)), ("Storage Boxes 3pc", "Storage", (49, 119)),
        ("LED Desk Lamp", "Lighting", (59, 179)), ("Ramadan Lantern", "Seasonal", (39, 149)),
        ("Pillow 2pk", "Bedding", (49, 149)), ("Laundry Detergent 5L", "Cleaning", (35, 59)),
        ("Dish Soap 1L", "Cleaning", (8, 14)), ("Air Freshener Oud", "Fragrance", (15, 35)),
        ("Water Dispenser", "Kitchen", (299, 799)), ("Food Containers 10pc", "Kitchen", (39, 99)),
        ("Wall Clock", "Decor", (49, 199)), ("Outdoor Chair", "Outdoor", (89, 299)),
    ],
    "Beauty": [
        ("Oud Perfume 100ml", "Fragrance", (199, 899)), ("Rose Attar 12ml", "Fragrance", (79, 299)),
        ("Bakhoor Blocks 50g", "Fragrance", (39, 149)), ("Sunscreen SPF50", "Skincare", (49, 139)),
        ("Argan Hair Oil", "Haircare", (39, 119)), ("Kohl Eyeliner", "Makeup", (29, 89)),
        ("Moisturiser 50ml", "Skincare", (59, 249)), ("Shampoo 400ml", "Haircare", (19, 59)),
        ("Lipstick", "Makeup", (39, 159)), ("Face Wash 150ml", "Skincare", (25, 79)),
        ("Henna Kit", "Seasonal", (19, 49)), ("Body Lotion 400ml", "Skincare", (29, 89)),
        ("Beard Oil", "Men's Grooming", (39, 99)), ("Razor 5pk", "Men's Grooming", (25, 69)),
        ("Nail Polish Set", "Makeup", (35, 99)),
    ],
    "Fashion": [
        ("Kandura White", "Menswear", (149, 499)), ("Ghutra", "Menswear", (59, 199)),
        ("Abaya Classic", "Womenswear", (199, 899)), ("Shayla", "Womenswear", (39, 149)),
        ("Sneakers", "Footwear", (179, 599)), ("Sandals", "Footwear", (69, 299)),
        ("Kids Eid Outfit", "Kidswear", (99, 299)), ("School Uniform Set", "Kidswear", (89, 199)),
        ("School Backpack", "Accessories", (79, 249)), ("Sunglasses", "Accessories", (99, 499)),
        ("Cotton T-Shirt", "Casual", (29, 89)), ("Linen Shirt", "Casual", (99, 249)),
        ("Sports Tracksuit", "Sportswear", (149, 399)), ("Leather Wallet", "Accessories", (79, 299)),
        ("Wrist Watch", "Accessories", (199, 1299)),
    ],
}
MARGIN = {"Grocery": (0.72, 0.82), "Beverages": (0.65, 0.78), "Electronics": (0.80, 0.90),
          "Home": (0.50, 0.65), "Beauty": (0.40, 0.58), "Fashion": (0.42, 0.60)}
BRANDS = {"Grocery": ["Sahra Select", "Oasis Farms", "Al Barakah", "Golden Palm"],
          "Beverages": ["Sahra Select", "Blue Wadi", "Karak House", "Fresh Dune"],
          "Electronics": ["Voltix", "Nuvo", "Zentra", "Kairo"],
          "Home": ["Sahra Home", "Majlis & Co", "Nest Gulf", "Qasr"],
          "Beauty": ["Oud Layali", "Sahra Beauty", "Musk Road", "Dune Glow"],
          "Fashion": ["Sahra Style", "Corniche", "Marina Line", "Falaj"]}


def build_products():
    products, pid = [], 1
    for cat, items in CATALOG.items():
        for name, sub, (lo, hi) in items:
            price = round(random.uniform(lo, hi), 2)
            cost = round(price * random.uniform(*MARGIN[cat]), 2)
            products.append({
                "product_id": f"P{pid:04d}", "product_name": name, "category": cat,
                "subcategory": sub, "brand": random.choice(BRANDS[cat]),
                "unit_price_aed": price, "unit_cost_aed": cost,
                "is_private_label": "true" if random.random() < 0.25 else "false",
                "launched_on": (date(2019, 1, 1) + timedelta(days=random.randint(0, 1800))).isoformat(),
            })
            pid += 1
    return products


# --------------------------------------------------------------------------- customers
FIRST = ["Ahmed", "Mohammed", "Fatima", "Aisha", "Omar", "Mariam", "Khalid", "Noura", "Yousef", "Hessa",
         "Rashid", "Layla", "Hamdan", "Shamma", "Saeed", "Reem", "Priya", "Arjun", "Ravi", "Anjali",
         "Imran", "Sana", "Bilal", "Zainab", "Maria", "Jose", "Angela", "Mark", "Grace", "Rodel",
         "James", "Emma", "Oliver", "Sophie", "Lucas", "Chloe", "Hassan", "Rania", "Tariq", "Dina",
         "Karim", "Yasmin", "Vikram", "Meera", "Farhan", "Ayesha", "Liam", "Olivia", "Ali", "Salma"]
LAST = ["Al Mansoori", "Al Nuaimi", "Al Shamsi", "Al Ketbi", "Al Zaabi", "Al Suwaidi", "Al Hashmi",
        "Al Falasi", "Khan", "Sheikh", "Hussain", "Qureshi", "Nair", "Menon", "Sharma", "Patel",
        "Reyes", "Santos", "Cruz", "Garcia", "Smith", "Brown", "Wilson", "Taylor", "Haddad",
        "Khoury", "Nasser", "Saleh", "Farouk", "Mansour", "Iyer", "Pillai", "Fernandes", "D'Souza"]
EMIRATE_WEIGHTS = [("Dubai", 0.38), ("Abu Dhabi", 0.30), ("Sharjah", 0.15), ("Ajman", 0.06),
                   ("Ras Al Khaimah", 0.05), ("Fujairah", 0.03), ("Umm Al Quwain", 0.03)]
CITY_FOR = {"Dubai": ["Dubai"], "Abu Dhabi": ["Abu Dhabi", "Abu Dhabi", "Al Ain"], "Sharjah": ["Sharjah"],
            "Ajman": ["Ajman"], "Ras Al Khaimah": ["Ras Al Khaimah"], "Fujairah": ["Fujairah"],
            "Umm Al Quwain": ["Umm Al Quwain"]}
TIERS = [("Bronze", 0.55), ("Silver", 0.27), ("Gold", 0.13), ("Platinum", 0.05)]


def wchoice(pairs):
    r, acc = random.random(), 0.0
    for v, w in pairs:
        acc += w
        if r <= acc:
            return v
    return pairs[-1][0]


def make_customer(cid, signup):
    fn, ln = random.choice(FIRST), random.choice(LAST)
    emirate = wchoice(EMIRATE_WEIGHTS)
    return {
        "customer_id": f"C{cid:05d}", "first_name": fn, "last_name": ln,
        "email": f"{fn.lower()}.{ln.lower().replace(' ', '').replace(chr(39), '')}{cid}@example.com",
        "phone": f"+97150{random.randint(1000000, 9999999)}",
        "city": random.choice(CITY_FOR[emirate]), "emirate": emirate,
        "loyalty_tier": wchoice(TIERS), "marketing_opt_in": "true" if random.random() < 0.62 else "false",
        "signup_date": signup.isoformat(),
        "updated_at": None,
    }


def build_customers():
    """Three CRM exports: Jan (existing base), Jul (H1 sign-ups + changes), Dec (H2 sign-ups)."""
    existing, signups_2025 = [], []
    cid = 1
    for _ in range(2600):                      # customers who joined 2020-2024
        c = make_customer(cid, date(2020, 1, 1) + timedelta(days=random.randint(0, 1826)))
        c["updated_at"] = "2025-01-01T00:00:00"
        existing.append(c); cid += 1
    for _ in range(500):                       # customers who join during 2025
        signups_2025.append(make_customer(cid, date(2025, 1, 2) + timedelta(days=random.randint(0, 362))))
        cid += 1
    # updated_at = when the CRM record last changed (for a new customer: the sign-up day)
    for c in signups_2025:
        c["updated_at"] = c["signup_date"] + "T00:00:00"
    h1 = [c for c in signups_2025 if c["signup_date"] < "2025-07-01"]   # arrive in the July export
    h2 = [c for c in signups_2025 if c["signup_date"] >= "2025-07-01"]  # arrive in the year-end export

    # 320 existing customers change tier or move emirate -> SCD Type 2 history
    changed = []
    for c in random.sample(existing, 320):
        n = dict(c)
        if random.random() < 0.7:
            order = ["Bronze", "Silver", "Gold", "Platinum"]
            i = order.index(n["loyalty_tier"])
            n["loyalty_tier"] = order[i + 1] if i < 3 else "Gold"
        else:
            em = wchoice(EMIRATE_WEIGHTS)
            n["emirate"], n["city"] = em, random.choice(CITY_FOR[em])
        changed_on = date(2025, 2, 1) + timedelta(days=random.randint(0, 148))
        n["updated_at"] = changed_on.isoformat() + "T12:00:00"
        changed.append(n)

    first_export = list(existing)
    random.shuffle(first_export)
    mid_export = changed + h1
    random.shuffle(mid_export)
    # for order generation use the *final* tier so loyal customers buy more
    final = {c["customer_id"]: c for c in existing + h1 + h2}
    for c in changed:
        final[c["customer_id"]] = c
    return first_export, mid_export, h2, list(final.values())


# --------------------------------------------------------------------------- calendar
RAMADAN = (date(2025, 3, 1), date(2025, 3, 29))
EID_FITR = (date(2025, 3, 30), date(2025, 4, 2))
EID_ADHA = (date(2025, 6, 5), date(2025, 6, 9))
DSF = [(date(2025, 1, 1), date(2025, 1, 12)), (date(2025, 12, 5), date(2025, 12, 31))]
WHITE_FRIDAY = (date(2025, 11, 24), date(2025, 11, 30))
BACK_TO_SCHOOL = (date(2025, 8, 15), date(2025, 9, 5))
NATIONAL_DAY = (date(2025, 12, 1), date(2025, 12, 3))


def within(d, rng):
    return rng[0] <= d <= rng[1]


def day_factor(d):
    f = 1.0
    if d.weekday() in (5, 6):  # Sat, Sun = UAE weekend
        f *= 1.28
    elif d.weekday() == 4:     # Friday half-day, busy evenings
        f *= 1.10
    if d.month in (7, 8):      # summer - many residents travel
        f *= 0.78
    if within(d, RAMADAN):
        f *= 1.18
    if within(d, EID_FITR) or within(d, EID_ADHA):
        f *= 1.55
    if any(within(d, r) for r in DSF):
        f *= 1.35
    if within(d, WHITE_FRIDAY):
        f *= 1.60
    if within(d, NATIONAL_DAY):
        f *= 1.25
    f *= 1 + 0.05 * math.sin(d.timetuple().tm_yday / 58.0)  # slow drift
    f *= 1 + (d.month - 1) * 0.012                           # mild growth through the year
    return f


def category_weights(d):
    w = {"Grocery": 0.44, "Beverages": 0.20, "Electronics": 0.07, "Home": 0.12, "Beauty": 0.09, "Fashion": 0.08}
    if within(d, RAMADAN):
        w["Grocery"] *= 1.45; w["Beverages"] *= 1.3; w["Home"] *= 1.2
    if within(d, EID_FITR) or within(d, EID_ADHA):
        w["Fashion"] *= 2.6; w["Beauty"] *= 2.0
    if within(d, BACK_TO_SCHOOL):
        w["Fashion"] *= 1.6; w["Electronics"] *= 1.7
    if within(d, WHITE_FRIDAY):
        w["Electronics"] *= 3.2
    if any(within(d, r) for r in DSF):
        w["Electronics"] *= 1.6; w["Fashion"] *= 1.5; w["Beauty"] *= 1.3
    return list(w.items())


def hour_for(d, store_format):
    if within(d, RAMADAN):  # evening shopping after iftar
        return random.choices(range(8, 24), weights=[1, 1, 2, 2, 2, 2, 2, 3, 3, 4, 6, 9, 11, 12, 11, 8])[0]
    base = [2, 3, 4, 5, 5, 5, 5, 5, 6, 7, 9, 10, 10, 8, 6, 3]
    if store_format == "Online":
        base = [3, 3, 4, 4, 5, 5, 5, 5, 5, 6, 7, 8, 9, 9, 8, 6]
    return random.choices(range(8, 24), weights=base)[0]


# --------------------------------------------------------------------------- orders
def build_orders(products, customers_all):
    by_cat = {}
    for p in products:
        by_cat.setdefault(p["category"], []).append(p)
    # Pareto: a small share of customers buy a lot
    cust = sorted(customers_all, key=lambda c: c["customer_id"])
    weights = [random.paretovariate(1.3) * {"Bronze": 1, "Silver": 1.6, "Gold": 2.4, "Platinum": 3.5}[c["loyalty_tier"]]
               for c in cust]
    store_w = [s[-1] for s in STORES]
    orders_by_month = {m: [] for m in range(1, 13)}
    seq = 0
    d = date(YEAR, 1, 1)
    while d.year == YEAR:
        n = int(random.gauss(175, 12) * day_factor(d))
        eligible_upto = d.isoformat()
        for _ in range(n):
            seq += 1
            store = random.choices(STORES, weights=store_w)[0]
            fmt = store[4]
            ts = datetime(d.year, d.month, d.day, hour_for(d, fmt), random.randint(0, 59), random.randint(0, 59))
            # customer: 18% guest (no loyalty card) in-store, 0% online
            cust_id = None
            if fmt == "Online" or random.random() > 0.18:
                for _try in range(6):
                    c = random.choices(cust, weights=weights)[0]
                    if c["signup_date"] <= eligible_upto:
                        cust_id = c["customer_id"]; break
            n_items = random.choices([1, 2, 3, 4, 5, 6, 8], weights=[30, 25, 18, 12, 8, 5, 2])[0]
            if fmt == "Express":
                n_items = min(n_items, 3)
            cats = category_weights(d)
            items = []
            for _i in range(n_items):
                cat = wchoice([(c, w / sum(x[1] for x in cats)) for c, w in cats])
                p = random.choice(by_cat[cat])
                qty = 1 if cat in ("Electronics",) else random.choices([1, 2, 3, 4, 6], weights=[55, 25, 10, 6, 4])[0]
                disc = 0
                if any(within(d, r) for r in DSF) or within(d, WHITE_FRIDAY):
                    disc = random.choice([0, 10, 15, 20, 25, 30])
                elif random.random() < 0.15:
                    disc = random.choice([5, 10, 15])
                items.append({"line_no": _i + 1, "product_id": p["product_id"], "qty": qty,
                              "unit_price": p["unit_price_aed"], "discount_pct": disc})
            status = random.choices(["COMPLETED", "RETURNED", "CANCELLED"], weights=[92, 4, 4])[0]
            pay = random.choices(["Card", "Cash", "Apple Pay", "Tabby BNPL"],
                                 weights=[50, 22, 20, 8] if fmt != "Online" else [55, 0, 30, 15])[0]
            o = {
                "order_id": f"SO-{YEAR}-{seq:07d}",
                "order_ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
                "store_id": store[0],
                "customer_id": cust_id,
                "channel": "Online" if fmt == "Online" else "In-Store",
                "payment_method": pay,
                "status": status,
                "currency": "AED",
                "items": items,
            }
            # ---------------- inject data-quality problems
            r = random.random()
            if r < 0.004:
                o["order_ts"] = ts.strftime("%d/%m/%Y %H:%M")           # second timestamp format
            elif r < 0.007:
                o["store_id"] = " " + store[0].lower() + " "            # messy key
            elif r < 0.012:
                o["status"] = random.choice(["completed", "Completed ", "returned"])
            elif r < 0.014:
                o["customer_id"] = "C99999"                             # orphan customer
            if random.random() < 0.004:
                o["items"][0]["qty"] = random.choice([0, -1, -2])        # bad quantity
            if random.random() < 0.002:
                o["items"][-1]["unit_price"] = None                     # missing price
            orders_by_month[d.month].append(o)
            if random.random() < 0.008:                                 # POS re-sent the order
                orders_by_month[d.month].append(json.loads(json.dumps(o)))
        d += timedelta(days=1)
    return orders_by_month


# --------------------------------------------------------------------------- write
def write_csv(path, rows, fields):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fields})


def main():
    products = build_products()
    first, mid, h2_new, all_customers = build_customers()

    sf = ["store_id", "store_name", "city", "emirate", "store_format", "latitude", "longitude",
          "opened_date", "size_sqm"]
    write_csv(os.path.join(OUT, "stores", "stores.csv"),
              [dict(zip(sf, s[:-1])) for s in STORES], sf)
    write_csv(os.path.join(OUT, "products", "products.csv"), products, list(products[0].keys()))
    cf = ["customer_id", "first_name", "last_name", "email", "phone", "city", "emirate",
          "loyalty_tier", "marketing_opt_in", "signup_date", "updated_at"]
    write_csv(os.path.join(OUT, "customers", "customers_2025-01-01.csv"), first, cf)
    write_csv(os.path.join(OUT, "customers", "customers_2025-07-01.csv"), mid, cf)
    write_csv(os.path.join(OUT, "customers", "customers_2025-12-31.csv"), h2_new, cf)

    orders = build_orders(products, all_customers)
    os.makedirs(os.path.join(OUT, "orders"), exist_ok=True)
    total = 0
    for m, rows in orders.items():
        with open(os.path.join(OUT, "orders", f"orders_{YEAR}_{m:02d}.json"), "w", encoding="utf-8") as f:
            for o in rows:
                f.write(json.dumps(o, ensure_ascii=False, separators=(",", ":")) + "\n")
        total += len(rows)
    print(f"stores={len(STORES)} products={len(products)} customers: first={len(first)} "
          f"mid={len(mid)} yearend={len(h2_new)} order_records={total}")


if __name__ == "__main__":
    main()

# COMMAND ----------

display(dbutils.fs.ls(OUT + "/orders"))
