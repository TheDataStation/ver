import pandas as pd

# TODO: sample in the reading process
def read_csv_columns_with_sampling(tbl_path, tbl_name, attrs, sample_size, sep):
    df = pd.read_csv(tbl_path, usecols=attrs, dtype="object", sep=sep)
    if len(df) > sample_size:
        df = df.sample(sample_size, random_state=0) #zz: fix random state so we will always sample the same rows from the same table
    df.columns = ["{}.{}".format(tbl_name, col) for col in df.columns]
    df = normalize(df)
    return df

def normalize(df):
    df = df.apply(lambda x: x.astype(str).str.strip().str.lower() if (x.dtype == 'object') else x)
    return df