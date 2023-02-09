import pandas as pd

def read_csv_columns_with_sampling(tbl_path, attrs, sample_size):
    df = pd.read_csv(tbl_path, usecols=attrs)
    if len(df) > sample_size:
        df = df.sample(sample_size)
    return df