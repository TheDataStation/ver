import pandas as pd

# TODO: sample in the reading process
def read_csv_columns_with_sampling(tbl_path, tbl_name, attrs, sample_size):
    df = pd.read_csv(tbl_path, usecols=attrs)
    if len(df) > sample_size:
        df = df.sample(sample_size)
    df.columns = ["{}.{}".format(tbl_name, col) for col in df.columns]
    return df