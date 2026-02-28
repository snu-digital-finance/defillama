# TVL

> Download, clean, and merge historical **Total Value Locked** data for every DeFi protocol and blockchain tracked by DefiLlama — no API key required.

---

## What is TVL?

**Total Value Locked (TVL)** is the total dollar amount of cryptocurrency deposited into a **DeFi** (Decentralized Finance) application at a given moment.
Think of it like the total cash held inside all the ATMs and vaults run by one bank — a higher number means more people trust and use the service.

### Glossary

| Term | Plain-English meaning |
| ---- | --------------------- |
| **TVL** | Total Value Locked — the aggregate USD value of crypto assets deposited in a protocol |
| **Protocol** | A software application on a blockchain that lets people lend, borrow, trade, or earn interest with crypto |
| **Chain** | A blockchain network (e.g., Ethereum, BNB Smart Chain, Solana) that protocols run on |
| **DefiLlama** | A free, open-source website that tracks TVL across thousands of DeFi protocols and chains |
| **API** | Application Programming Interface — a URL you call to receive structured data in return |
| **CSV** | Comma-Separated Values — a plain text file where each row is a record and columns are separated by commas |
| **Slug** | A URL-safe version of a name, e.g., `"Aave V3"` becomes `aave-v3` |
| **Timestamp** | The exact date and time a measurement was recorded, stored as a number (Unix epoch) or a formatted string |
| **Polars** | A fast Python data-processing library, similar to pandas but optimised for large files |
| **STEP** | The number of protocol or chain files processed in one batch before writing an intermediate result |

---

## Prerequisites

1. **Python 3.8 or newer** installed on your machine.
2. Install required packages:
   ```
   pip install requests pandas openpyxl polars
   ```
3. **No API key is needed.** DefiLlama's TVL endpoints are public and free.
4. Open **`aggTVL.ipynb`** in Jupyter Notebook or JupyterLab and run cells in order from top to bottom.
5. There are no other notebook sections that must run first — this notebook is self-contained.

---

## Step-by-step instructions

### Cell 1 — Import libraries

**Cell action:** Run this cell first to load every library used in the notebook.

```python
import requests
import pandas as pd
import os
from datetime import datetime
from io import StringIO
import json
```

This cell loads:
- `requests` — sends HTTP requests to the DefiLlama API.
- `pandas` — loads and transforms tabular data.
- `os` — creates folders and lists files on disk.
- `datetime` — converts Unix timestamps to human-readable dates.
- `StringIO` — treats an in-memory string as if it were a file, so `pd.read_csv` can parse CSV data received over the network.
- `json` — parses JSON responses.

---

### Cell 2 — Define `get_list`

**Cell action:** Run this cell to define a helper function. Nothing is downloaded yet.

```python
def get_list(url: str, types: str = None):
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    if types == "protocol":
        df["protocol"] = df["name"].apply(
            lambda x: x.lower().replace(" ", "-"))
    df.to_excel(f"{types}.xlsx", index=False)
    print(df.shape, df.columns)
    return df
```

**`get_list`** calls a DefiLlama API URL, converts the JSON response into a pandas DataFrame, and saves it as an Excel file.
When `types="protocol"` it also creates a `protocol` column that turns the display name (e.g., `"Aave V3"`) into the URL slug (`aave-v3`) used to fetch per-protocol data.

---

### Cell 3 — Define `request_csv`

**Cell action:** Run this cell to define a second helper function. Nothing is downloaded yet.

```python
def request_csv(url: str):
    response = requests.get(url, allow_redirects=True)
    data = response.content.decode("utf-8")
    if data == "Internal server error":
        return None
    return pd.read_csv(StringIO(data), sep=",")
```

**`request_csv`** fetches a CSV file from a URL and returns it as a pandas DataFrame.
If DefiLlama responds with `"Internal server error"` the function returns `None` so the calling code can skip that entry gracefully.

---

### Cell 4 — Download the protocol list

**Cell action:** Run this cell to fetch every protocol tracked by DefiLlama and save the list.

```python
# https://api-docs.defillama.com/#tag/tvl/get/protocols
url = "https://api.llama.fi/protocols"

df_protocol = get_list(url, "protocol")

df_protocol.head()
```

This calls `get_list` with the `/protocols` endpoint, which returns a JSON array of ~6 000 protocols.
The result is stored in `df_protocol` and saved to **`protocol.xlsx`** in the working directory.
The cell prints the DataFrame shape and column names, then displays the first five rows.

---

### Cell 5 — Download per-protocol TVL CSVs

**Cell action:** Run this cell to download a historical TVL file for each protocol. This will take several minutes.

```python
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

df_protocol = pd.read_excel("protocol.xlsx")

failed = []
os.makedirs("df_protocol", exist_ok=True)

for v in df_protocol["protocol"]:
    url = "https://api.llama.fi/dataset/%s.csv" % v
    df = request_csv(url)
    if df is None or df.empty:
        failed.append(v)
        print(v, "No data")
        continue

    try:
        df = df.reset_index()
        total = df.iloc[1].astype(str).str.contains("Total")
        tvl = df.iloc[2].astype(str).str.contains("TVL")
        col = df.columns[total & tvl]

        if len(col) == 0:
            print(v, "Columns not found")
            continue
        if len(col) > 1:
            print(v, "Multiple columns found")
            print(col)
            continue

        df = df.iloc[4:]
        df["Timestamp"] = df["Timestamp"].apply(
            lambda x: datetime.fromtimestamp(x))
        col = col.item()
        df = df[["Date", "Timestamp", col]]
        df.to_csv(f"df_protocol/{v}.csv", index=False)
    except Exception as e:
        print(v, e)

failed
```

For each protocol slug, the code calls `https://api.llama.fi/dataset/<slug>.csv`.
The raw CSV has several header rows; the code inspects rows 1 and 2 to locate the column that represents the protocol's total TVL (containing both `"Total"` and `"TVL"`).
It then trims the header rows, converts the `Timestamp` column from a Unix number to a Python datetime, and saves the three-column result to `df_protocol/<slug>.csv`.
Any protocol that returns no data or cannot be parsed is added to the `failed` list, which is printed at the end.

---

### Cell 6 — Download the chain list

**Cell action:** Run this cell to fetch the list of all blockchains tracked by DefiLlama.

```python
url = "https://api.llama.fi/v2/chains"
df_chain = get_list(url, "chain")
df_chain.head()
```

This calls the `/v2/chains` endpoint and saves the result to **`chain.xlsx`**.
The DataFrame `df_chain` contains one row per blockchain and is used in the next cell to download per-chain TVL data.

---

### Cell 7 — Download per-chain TVL CSVs

**Cell action:** Run this cell to download a historical TVL file for each blockchain. This will take a few minutes.

```python
failed = []
os.makedirs("df_chain", exist_ok=True)

for name in df_chain["name"]:
    name = name.replace(" ", "_")
    try:
        url = "https://api.llama.fi/simpleChainDataset/%s?" % name
        df = request_csv(url)
        if df is None or df.empty:
            failed.append(name)
            continue

        df = df.head(1).T.iloc[1:].reset_index()
        df["date"] = df["index"].map(
            lambda x: datetime.strptime(x, '%d/%m/%Y'))
        df["date"] = df["date"].dt.strftime('%Y-%m-%d')
        df[name] = df[0]
        df = df[["date", name]]
        df.to_csv(f"df_chain/{name}.csv", index=False)

    except Exception as e:
        print(name, e)
```

For each chain, the code calls `https://api.llama.fi/simpleChainDataset/<chain>?`.
The response is a wide-format CSV where each column is a date; the code **transposes** it so dates become rows, reformats the date strings to `YYYY-MM-DD`, and saves the two-column result to `df_chain/<chain>.csv`.

---

### Cell 8 — Remove zero-TVL files

**Cell action:** Run this cell to delete files whose entire TVL history is zero.

```python
for v in os.listdir("df_protocol"):
    df = pd.read_csv(f"df_protocol/{v}")
    df = df.iloc[:, 2:].fillna(0)
    try:
        if df.sum().sum() == 0:
            os.remove(f"df_protocol/{v}")
    except Exception as e:
        print(v, e)

for v in os.listdir("df_chain"):
    df = pd.read_csv(f"df_chain/{v}")
    df = df.iloc[:, 2:].fillna(0)
    try:
        if df.sum().sum() == 0:
            os.remove(f"df_chain/{v}")
    except Exception as e:
        print(v, e)
```

For every CSV in `df_protocol/` and `df_chain/`, the code reads the numeric value columns (skipping the date/timestamp columns), replaces missing values with `0`, and checks whether the grand total is still `0`.
If so, the file is deleted — it contains no useful TVL information.

---

### Cell 9 — Count remaining chain files

**Cell action:** Run this cell to confirm how many chain files survived the zero-TVL filter.

```python
len(os.listdir("df_chain"))
```

Prints the number of CSV files still present in `df_chain/`. Expect a number close to the total number of chains minus those with no recorded TVL.

---

### Cell 10 — Count remaining protocol files

**Cell action:** Run this cell to confirm how many protocol files survived the zero-TVL filter.

```python
len(os.listdir("df_protocol"))
```

Prints the number of CSV files still present in `df_protocol/`. Expect a number in the thousands.

---

### Cell 11 — Import Polars and define merge helpers

**Cell action:** Run this cell to load the Polars library and define helper functions used during the merge step.

```python
import os
import polars as pl
from datetime import datetime, timedelta

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def leave_first_last(
    df: pl.DataFrame, col: str
) -> pl.DataFrame:
    df = df.with_columns([
        pl.arange(0, df.shape[0], eager=True).alias("index")
    ])
    return df.filter(
        (pl.col(col) != pl.col(col).shift(1)) |
        (pl.col(col) != pl.col(col).shift(-1)) |
        (pl.col("index") == 0) |
        (pl.col("index") == (df.shape[0] - 1))
    ).drop("index")


def drop_null_rows(
    df: pl.DataFrame
) -> pl.DataFrame:
    condition = pl.lit(False)
    for col in [col for col in df.columns if col not in ['date', 'hourly_date', 'Timestamp']]:
        condition = condition | pl.col(col).is_not_null()
    return df.filter(condition)


def change_hourly(
    df: pl.DataFrame, col: str = "hourly_date"
) -> pl.DataFrame:
    return df.group_by(col).agg([
        pl.col(company).mean().alias(company)
        for company in df.columns if company != col
    ]).sort(col)


def cast_column_type_without_date(
    df: pl.DataFrame, dtype: object = float, start_colnum: int = 1
) -> pl.DataFrame:
    for col in df.columns[start_colnum:]:
        df = df.with_columns(pl.col(col).cast(dtype))
    return df


def cast_datetime(
    df: pl.DataFrame, has_ms=False, dt_format: str = None, col: str = "date"
) -> pl.DataFrame:
    return df.with_columns([
        pl.col(col).str.to_datetime(
            format=(dt_format if dt_format else (
                "%Y-%m-%dT%H:%M:%S.%f" if has_ms else DATE_FORMAT)))
    ])


def truncate_datetime(
    df: pl.DataFrame, col: str = "date", trun: str = "1h"
) -> pl.DataFrame:
    return df.with_columns(pl.col(col).dt.truncate(trun))
```

These helper functions are used to clean and reshape data during the merge:
- **`leave_first_last`** — for a column where many consecutive rows share the same value, it keeps only the first and last occurrence of each run, reducing file size.
- **`drop_null_rows`** — removes rows where every value column is null.
- **`change_hourly`** — groups rows by hour and averages the values within each hour.
- **`cast_column_type_without_date`** — converts all non-date columns to a specified numeric type (default `float`).
- **`cast_datetime`** — parses a string column into a proper datetime type.
- **`truncate_datetime`** — rounds a datetime column down to a given time unit (e.g., `"1d"` for day, `"1h"` for hour).

---

### Cell 12 — Merge all files in batches

**Cell action:** Run this cell to combine the individual per-protocol and per-chain CSVs into batched merged files.

```python
def merge_files(folder_name, STEP=1000) -> None:
    files = os.listdir(folder_name)
    results = []
    os.makedirs(f"result_{folder_name}", exist_ok=True)
    print("Total ephocs:", len(files)//STEP + 1)
    for i in range(0, len(files), STEP):
        for d in files[i:i+STEP]:
            df = pl.read_csv(
                f"{folder_name}/{d}").sort("Timestamp").drop(["Date"])
            if df.shape[1] > 2:
                print(d, df.shape)
            df = cast_column_type_without_date(
                df).rename({df.columns[1]: "tvl"})
            df = cast_datetime(df, has_ms=False, col="Timestamp")
            df = truncate_datetime(df, "Timestamp", "1d")
            df = df.with_columns(pl.lit(d.replace(".csv", "")).alias("name"))
            results.append(df)

        result = pl.concat(results, how="vertical").sort("Timestamp")
        fpath = f"result_{folder_name}/{STEP}_{i}.csv"
        result.write_csv(fpath)
        print(fpath, result.shape)


merge_files("df_protocol", STEP=1000)
merge_files("df_chain", STEP=1000)
```

**`merge_files`** processes files in groups of `STEP` (1 000 by default).
It prints the total number of epochs (batches) at the start — note that the notebook spells this `"ephocs"` which is a typo in the original code.
For each file it:
1. Reads the CSV and drops the `Date` column (keeping only `Timestamp` and the TVL value).
2. Renames the TVL value column to `tvl` for consistency.
3. Parses the `Timestamp` column into a datetime and truncates it to the day.
4. Adds a `name` column containing the protocol or chain slug.

After processing each batch of files, it concatenates all rows and writes one merged CSV to `result_df_protocol/` or `result_df_chain/`.

---

## Output columns explained

### `df_protocol/<protocol-slug>.csv`

| Column | Meaning |
| ------ | ------- |
| `Date` | Calendar date of the measurement (`YYYY-MM-DD`) |
| `Timestamp` | Date and time of the measurement as a Python datetime string |
| `<tvl-column>` | Total TVL in USD for this protocol on this date; the column name varies by protocol (e.g., `"Total TVL USD"`) |

### `df_chain/<chain-name>.csv`

| Column | Meaning |
| ------ | ------- |
| `date` | Calendar date of the measurement (`YYYY-MM-DD`) |
| `<chain-name>` | Total TVL in USD for this blockchain on this date; the column name matches the chain (e.g., `Ethereum`) |

### `result_df_protocol/<batch>.csv` and `result_df_chain/<batch>.csv`

| Column | Meaning |
| ------ | ------- |
| `Timestamp` | Date of the measurement, truncated to day precision |
| `tvl` | Total TVL in USD for this protocol or chain on this date |
| `name` | The slug or name that identifies the protocol or chain (e.g., `aave-v3`) |

---

## Output folder layout

```
defillama/
├── aggTVL.ipynb            # Main notebook (run this)
├── protocol.xlsx           # List of all protocols fetched from DefiLlama
├── chain.xlsx              # List of all chains fetched from DefiLlama
├── df_protocol/            # One CSV per protocol (Date, Timestamp, TVL)
│   ├── aave-v3.csv
│   ├── lido.csv
│   └── ...
├── df_chain/               # One CSV per blockchain (date, chain-TVL)
│   ├── Ethereum.csv
│   ├── BSC.csv
│   └── ...
├── result_df_protocol/     # Batched merged protocol files (Timestamp, tvl, name)
│   ├── 1000_0.csv
│   ├── 1000_1000.csv
│   └── ...
└── result_df_chain/        # Batched merged chain files (Timestamp, tvl, name)
    ├── 1000_0.csv
    └── ...
```

---

## Troubleshooting

| Problem | Likely cause | Fix |
| ------- | ------------ | --- |
| `ModuleNotFoundError: No module named 'polars'` | Polars is not installed | Run `pip install polars` in your terminal and restart the Jupyter kernel |
| A protocol prints `"No data"` and is added to `failed` | DefiLlama does not have TVL history for that slug | This is normal for newly listed or inactive protocols; review the `failed` list at the end of Cell 5 and ignore or investigate individually |
| Cell 5 raises `"Columns not found"` for a protocol | The raw CSV layout for that protocol differs from the expected format | Open `df_protocol/<slug>.csv` manually and inspect rows 0–4; the column detection logic may need adjustment for that entry |
| `FileNotFoundError: protocol.xlsx not found` | Cell 4 was not run before Cell 5, or the working directory is wrong | Re-run Cell 4 first; check your current directory with `import os; os.getcwd()` |
| Cell 12 raises a `SchemaError` about column types | A TVL column contains non-numeric strings (e.g., `"N/A"`) | Add error handling in `cast_column_type_without_date` or pre-clean the CSV before merging |
| Merge output files are unexpectedly large | `STEP` is set too high, causing many files to accumulate in memory | Reduce `STEP` to `500` or lower in the `merge_files` call |

---

## API reference

All data is fetched from the **DefiLlama public REST API** at `https://api.llama.fi`.
No authentication or API key is required.

### Endpoint 1 — List all protocols

```
GET https://api.llama.fi/protocols
```

Used by `get_list(url, "protocol")` in Cell 4.

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| *(none)* | — | — | No query parameters; the endpoint returns all protocols |

Returns a JSON array where each item represents one protocol. Key fields used by this notebook: `name` (display name) and derived `protocol` (URL slug).

---

### Endpoint 2 — Download per-protocol TVL CSV

```
GET https://api.llama.fi/dataset/{protocol}.csv
```

Used by `request_csv(url)` in Cell 5.

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `{protocol}` | string (path) | Yes | The URL slug of the protocol, e.g., `aave-v3` |

Returns a multi-row-header CSV. Rows 0–3 are metadata; data starts at row 4. Columns include `Date`, `Timestamp`, and one or more TVL sub-columns.

---

### Endpoint 3 — List all chains

```
GET https://api.llama.fi/v2/chains
```

Used by `get_list(url, "chain")` in Cell 6.

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| *(none)* | — | — | No query parameters; the endpoint returns all tracked chains |

Returns a JSON array. Key fields used: `name` (blockchain display name).

---

### Endpoint 4 — Download per-chain TVL CSV

```
GET https://api.llama.fi/simpleChainDataset/{chain}?
```

Used by `request_csv(url)` in Cell 7.

| Parameter | Type | Required | Description |
| --------- | ---- | -------- | ----------- |
| `{chain}` | string (path) | Yes | The chain name with spaces replaced by underscores, e.g., `BNB_Smart_Chain` |

Returns a wide-format CSV where the first row contains date strings in `DD/MM/YYYY` format and each column holds the TVL value for that date.

---

## `get_list` function signature

```python
def get_list(url: str, types: str = None) -> pd.DataFrame
```

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `url` | `str` | *(required)* | Full DefiLlama API endpoint URL |
| `types` | `str` | `None` | Output file base name; when `"protocol"` also derives the `protocol` slug column |

## `request_csv` function signature

```python
def request_csv(url: str) -> pd.DataFrame | None
```

| Parameter | Type | Default | Description |
| --------- | ---- | ------- | ----------- |
| `url` | `str` | *(required)* | Full URL to a DefiLlama CSV endpoint |

Returns a pandas DataFrame on success, or `None` if the server returns an error.

---

[← Back to project notebooks](aggTVL.ipynb)
