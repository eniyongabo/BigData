from datetime import datetime
import pandas as pd
import random
from plotly.graph_objs.scatter import Legendgrouptitle
import plotly.graph_objs as go


class Event:
    def __init__(self) -> None:
        self.dates = []
        self.views = []
        self.purchases = []
        self.carts = []
        self.l = 0

    def append(self, data: dict):
        self.dates.append(datetime.now())
        self.views.append(data.get("view", 0))
        self.purchases.append(data.get("purchase", 0))
        self.carts.append(data.get("cart", 0))
        self.l += 1
        if self.l > 15:
            self.pop()

    def pop(self, idx=0):
        self.dates.pop(idx)
        self.views.pop(idx)
        self.purchases.pop(idx)
        self.carts.pop(idx)

    def append_random(self):
        self.append(
            {
                "view": random.randint(0, 20),
                "purchase": random.randint(0, 20),
                "cart": random.randint(0, 20),
            }
        )

    def fig(self):
        # print(self.views)
        # print(self.purchases)
        # print(self.carts)
        return go.Figure(
            data=[
                go.Scatter(
                    x=self.dates,
                    y=self.views,
                    legendgroup="view",
                    name="View",
                    legendgrouptitle=Legendgrouptitle(text="View"),
                ),
                go.Scatter(
                    x=self.dates,
                    y=self.carts,
                    name="Cart",
                    legendgroup="cart",
                    legendgrouptitle=Legendgrouptitle(text="Cart"),
                ),
                go.Scatter(
                    x=self.dates,
                    y=self.purchases,
                    name="Purchase",
                    legendgroup="purchase",
                    legendgrouptitle=Legendgrouptitle(text="Purchase"),
                ),
            ],
            layout=go.Layout(title="Real Time user behavior in the store app"),
        )

    def df(self):
        return pd.DataFrame.from_dict(
            {
                "date": self.date,
                "view": self.views,
                "purchase": self.purchases,
                "cart": self.carts,
            }
        )
