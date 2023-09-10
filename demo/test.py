import pandas as pd
from sodapy import Socrata

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.cityofchicago.org", None, timeout=1000)

datasets = client.datasets(only=["dataset"])

print(len(datasets))

# files = []
# dfs = []
# counter = 0
# for dataset in datasets:
#     counter += 1
#     if counter < 555:
#         continue
#     print(dataset)
#     result = client.get(dataset["resource"]["id"])
#     df = pd.DataFrame.from_records(result)
#     name = dataset["resource"]["name"]
#     name = name.replace("/", "_")
#     df.to_csv(f"test_data/{name}.csv", index=False)
    # dfs.append(df)

for dataset in datasets:
    for name in dataset["resource"]["columns_name"]:
        if "ZIP" in name.upper():
            print(dataset["resource"]["name"], dataset["resource"]["columns_name"])
            continue
