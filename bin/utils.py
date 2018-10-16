def unique_col_name(target_col, columns):
    # creates a unique new name for column
    # base on target_col string and appending suffix
    i = 0
    while target_col in columns:
        i += 1
        target_col = target_col + f'_{i}'
    return target_col

def facilitator(self, func):
## write a pickable closure in python
    def output(df, func, target_col, new_col = ''):
        ## ensures new_col exists
        new_col = unique_col_name(target_col, df.columns) if (new_col not in df.colums) or (new_col == '') else new_col
        ## create new column with function passed
        df[new_col] = df[target_col].apply(func)
        ## return the frame
        return df
    return output