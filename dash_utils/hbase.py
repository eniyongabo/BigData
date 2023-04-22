import happybase

import pandas as pd
brands = [
    "samsung",
    "apple",
    "asus",
    "msi",
    "gigabyte",
    "dell",
    "hp",
    "lenovo",
    "sony",
    "intel",
]


class HBaseReader:
    def __init__(self) -> None:
        self.connection = happybase.Connection("localhost")

    def read(self):
        try:
            table = self.connection.table("electronic-analytics")
            row = table.row(b"brand", columns=["report"])
            data = {"brand": [], "sales": []}
            for brand in brands:
                data["brand"].append(brand)
                data["sales"].append(int(float(row[f"report:{brand}".encode()])))
            print(data)
            return pd.DataFrame.from_dict(data)
        except Exception as e:
            print(e)
            return None




