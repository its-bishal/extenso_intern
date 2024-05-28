

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from helper import column_remover, unique_id


# return similarity matrix between two dataframes
def text_vectorizer(input_df1, input_df2):
    tfidf_vectorizer = TfidfVectorizer()

    combined_texts = input_df1.tolist() + input_df2.tolist()

    tfidf_vectorizer.fit(combined_texts)

    # transform each column
    tf_idf_matrix_t1 = tfidf_vectorizer.transform(input_df1)
    tf_idf_matrix_t2 = tfidf_vectorizer.transform(input_df2)

    similarity = cosine_similarity(tf_idf_matrix_t1, tf_idf_matrix_t2)
    return pd.DataFrame(similarity)


# combine the two dataframes based on the unique ID created as a combination of multiple columns
def combined(input_df1, input_df2, col_df1, col_df2, similarity_df):
    max_idx = similarity_df.idxmax(axis=1)
    threshold = 0.0
    similarity_mask = similarity_df.max(axis=1) >= threshold

    combined_df = pd.DataFrame({
        col_df1: input_df1[col_df1].values,
        col_df2: [input_df2.loc[idx, col_df2] if mask else None for idx, mask in
                  zip(max_idx.values, similarity_mask)]
    })

    resulted = pd.merge(pd.merge(input_df1, combined_df, on=col_df1, how='left'), input_df2, on=col_df2,
                        how='inner')
    resulted.drop(columns=[col_df2], inplace=True)
    column_remover(resulted, input_df1, input_df2)
    return resulted


# create the final combined dataframe based on the unique ID using above functions
def result(df1, df2):
    df1['combined1'] = unique_id(df1)
    df2['combined2'] = unique_id(df2)

    similarity_df = text_vectorizer(df1['combined1'], df2['combined2'])
    final_result = pd.DataFrame(combined(df1, df2, 'combined1', 'combined2', similarity_df))
    return final_result
