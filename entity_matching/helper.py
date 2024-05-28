
import pandas as pd


# this file conatains helper functions used for processing of the dataframes
def unique_id(df):
    combined1 = df.apply(lambda row: f"{row['Date of Birth']} {row['Name']} {row['Father_Name']}", axis=1)
    return combined1


def column_remover(df, input_df1, input_df2):
    columns = list(set(input_df1.columns).union(set(input_df2.columns)))
    column_pairs = [(col, f"{col}_x", f"{col}_y") for col in columns]

    for new_col, col_x, col_y in column_pairs:
        if col_x in df.columns and col_y in df.columns:
            df[new_col] = df[col_x].combine_first(df[col_y])
            df.drop([col_x, col_y], axis=1, inplace=True)

    return df


def transform(df):
    return df.map(
        lambda x: x.replace(',', '').replace(' ', '').strip() if isinstance(x, str) else '' if pd.isna(x) else x)


def create_soup(df, df_, soup, soup_name):
    df[soup_name] = df_[soup].apply(lambda x: ' '.join(x.values.astype(str)).lower(), axis=1)

