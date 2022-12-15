import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

data = pd.read_csv("test_output_data_stats_history.csv", delimiter=",")
data = data[data["Name"] != "Aggregated"]
data["Timestamp"] = pd.to_datetime(data["Timestamp"], unit="s")

data = pd.melt(
    data,
    id_vars=(
        "Timestamp",
        "Name",
    ),
    # value_vars=["50%", "75%", "90%", "95%", "99%"],
    value_vars=["50%"],
    var_name="percentile",
)

sb.lineplot(
    data=data,
    x="Timestamp",
    y="value",
    hue="Name",
    style="percentile",
)
plt.axvline(x=pd.to_datetime("2022-12-15 10:36:08"), color="r", label="defrag. phase I")
plt.legend()
plt.show()
