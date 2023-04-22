from datetime import datetime
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd

from dash_utils.event import Event
from dash_utils.listener import EventListener

# df = pd.read_csv("plotly/data.csv")

app = Dash(__name__)

# events = pd.DataFrame(columns=["date", "view", "purchase", "cart"])
listener = EventListener(on_new_event=lambda data: event.append(data))
event = Event()


app.layout = html.Div(
    [
        html.H1(children="Title of Dash App", style={"textAlign": "center"}),
        # dcc.Dropdown(df.country.unique(), "Canada", id="dropdown-selection"),
        dcc.Graph(id="graph-content"),
        dcc.Interval(
            id="interval-component", interval=1 * 500, n_intervals=0  # in milliseconds
        ),
    ]
)


@app.callback(
    Output("graph-content", "figure"), Input("interval-component", "n_intervals")
)
def update_graph_live(n):
    print("Heartbeat")
    listener.consume()
    return event.fig()


if __name__ == "__main__":
    app.run(debug=True, dev_tools_ui=True)
