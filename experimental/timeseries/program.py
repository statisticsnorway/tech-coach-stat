import pandas as pd
from ssb_timeseries.dataset import Dataset


dates = [
    pd.Timestamp("2026-05-01"),
    pd.Timestamp("2026-05-03"),
    pd.Timestamp("2026-05-04"),
]

df_a = pd.DataFrame({"valid_at": dates, "a": [1.1, 1.3, 1.4]})
df_b = pd.DataFrame({"valid_at": dates, "a": [2.5, 2.6, 2.7]})

ds_a = Dataset(name="ds_a", data = df_a, data_type="simple")
ds_b = Dataset(name="ds_b", data = df_b, data_type="simple")

ds_c = ds_a + ds_b
ds_a.save()
ds_a.snapshot()