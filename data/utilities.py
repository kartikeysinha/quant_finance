"""
Common function to regarding data processes
"""

import os
import pandas as pd
from inputimeout import inputimeout


"""
Functions around loading data in .csv files.
"""

def _is_df_subset(new_df, df):
    """
    check if new_df is a subset of df
    """
    combined_df = pd.concat([new_df, df], ignore_index=True)

    return combined_df.duplicated(keep=False).any()


def _find_unique_data(new_df, old_df):
    """
    Merge 2 dataframes, ensuring all the entries are unique. 
    If data exists in both dataframes, delete data from old
    """

    df = old_df.merge(new_df, how='left', indicator=True)
    return df[df['_merge'] == 'left_only'].drop(columns=['_merge'])


def _fix_indices(df : pd.DataFrame) -> None:
    """
    Reset DataFrame indices to ensure consistent indexing.
    If index is RangeIndex, drops the old index. Otherwise, keeps it as a column.
    
    Args:
        df (pd.DataFrame): DataFrame to fix indices for
    """
    if isinstance(df.index, pd.RangeIndex):
        df.reset_index(drop=True, inplace=True)
    else:
        df.reset_index(drop=False, inplace=True)


def update_stored_data(
        new_data :pd.DataFrame,  
        old_data: pd.DataFrame = None, 
        file_path :str = None,
        file_name :str = None,
        cols_to_compare :list[str] = None
    ) -> pd.DataFrame:
    """
    Update old data. Run some quality checks before.
    """
    
    assert (old_data is not None) or (file_path is not None), "please provide old_data or file_path"

    if old_data is None:
        old_data = pd.read_csv(file_path + file_name)
    
    _fix_indices(old_data)
    _fix_indices(new_data)

    # Store a copy in memory and given file (if provided) in case process fails.
    tmp_old = old_data.copy()   
    if file_path:
        fp = os.path.join(file_path, file_name.split('.')[0] + '_temp.csv')
        old_data.to_csv( fp )      # store old data in temp file 

    if cols_to_compare is None or len(cols_to_compare) == 0:
        cols_to_compare = new_data.columns
    
    # check if new_data is a subset of old_data
    # print(new_data[cols_to_compare])
    if _is_df_subset(new_data[cols_to_compare], old_data[cols_to_compare]):
        # Ask user if they want to overwrite data
        prompt = f"The new_df has duplicates based on cols_to_compare. Do you want to overwrite the data? Y or N?\n"
        try:
            usr_res = inputimeout(prompt, 5)
        except:
            usr_res = 'n'       # default to 'no'

        if usr_res.lower().startswith('n'):
            print("Aborting data process based on user request.")
            return

        # override
        tmp_df = _find_unique_data(new_data[cols_to_compare], old_data[cols_to_compare])
        if len(cols_to_compare) > 0 and len(cols_to_compare) < len(new_data.columns):
            retval = tmp_df.merge(old_data, how='left')
            old_data = retval.loc[~retval.duplicated()].reset_index(drop=True)
        else:
            old_data = tmp_df
    
    comb_df = pd.concat([old_data, new_data], axis=0).reset_index(drop=True)

    if len(cols_to_compare) > 0 and len(cols_to_compare) < len(new_data.columns):
        comb_df.set_index(cols_to_compare, inplace=True)

    if file_path:
        comb_df.to_csv(file_path + file_name)

    return comb_df
