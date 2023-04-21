import csv

# 885129
db = []
fieldnames = []
brands = dict()
popular_brands = [
    x.strip().lower()
    for x in "Samsung, Apple, Asus, MSI, Gigabyte, Dell, HP, Lenovo, Sony, Intel".split(
        ","
    )
]
events = {}
with open("events.csv", newline="") as csvfile:
    reader = csv.DictReader(csvfile)
    fieldnames = reader.fieldnames
    for row in reader:
        event = row["event_type"]
        events[event] = events.get(event, 0) + 1
        brand = row["brand"]
        if brand in popular_brands:
            brands[brand] = brands.get(brand, 0) + 1
            db.append(row)

print(events)
for b, c in sorted(brands.items(), key=lambda k: (k[1], k[0]), reverse=True):
    print(b, c)


with open("output.csv", "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()

    for row in db:
        writer.writerow(row)


# ${HBASE_HOME}/bin/hbase start master