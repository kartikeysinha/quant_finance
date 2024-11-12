"""
Common function to regarding data processes
"""

import os
import pandas as pd
from inputimeout import inputimeout

"""
Functions around loading data in pickle files.
"""

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
    assert (not file_name.endswith(".csv"))

    # Attach pickle to file name if not already there.
    file_name = file_name if file_name.endswith(".pickle") else file_name + ".pickle"

    # If `old_data` is not provided, read it in.
    if old_data is None:
        old_data = pd.read_pickle(file_path + file_name)
        if 'date' in old_data.columns:
            old_data['date'] = pd.to_datetime(old_data['date'])
    
    # Standardize the indices in both the new and old data
    _fix_indices(old_data)
    _fix_indices(new_data)

    # Store a copy in the given file_path (if provided) in case process fails.
    if file_path:
        fp = os.path.join(file_path, file_name.split('.')[0] + '_temp.pickle')
        old_data.to_pickle( fp )      # store old data in temp file 

    # If no columns to compare is provided, we check for duplicates in the entire dataframe.
    if cols_to_compare is None or len(cols_to_compare) == 0:
        cols_to_compare = new_data.columns
    
    # Find duplicates if they exist
    new_data = new_data.set_index(cols_to_compare)
    old_data = old_data.set_index(cols_to_compare)
    common_indices = list(set(old_data.index).intersection(set(new_data.index)))

    # check if new_data is a subset of old_data
    if len(common_indices) > 0:
        # Ask user if they want to overwrite data
        prompt = f"The new_df has duplicates based on cols_to_compare. Do you want to overwrite the data? Y or N?\n"
        try:
            usr_res = inputimeout(prompt, 5)
        except:
            usr_res = 'n'       # default to 'no'

        if usr_res.lower().startswith('n'):
            print("The overlapping data will not be overwritten. Any data data will be appended.")
            new_data.drop(common_indices, axis=0, inplace=True)     # delete from new data
        elif usr_res.lower().startswith('y'):
            old_data.drop(common_indices, axis=0, inplace=True)     # delete from old data
        else:
            raise ValueError(f"Input not recognized: {usr_res}\nAborting...")
        
    comb_df = pd.concat([old_data, new_data], axis=0).sort_index()

    if file_path:
        comb_df.to_pickle(file_path + file_name)

    return comb_df
