import numpy as np
import pandas as pd
import multiprocessing as mp
from pathos.multiprocessing import ProcessingPool as Pool


class multiprocessor:
    def __init__(self,
                 partitions=mp.cpu_count(),  # how many sub-dataframes to split into
                 cores=mp.cpu_count()):  # num_cores is used for the MultiProcessor Core
        self.partitions = partitions
        self.cores = cores

    def preprocessing_checks(self, df, func):
        if not isinstance(df, type(pd.DataFrame())):
            raise ('parallelize_dataframe\'s first argument needs to be a Pandas DataFrame.')
        elif not callable(func):
            raise ('parallelize_dataframe\'s second argument needs to be a Python3 Function.')
        else:
            return True

    def parallel_apply(self, df, func):
        # add try statement re function not returning a DataFrame
        if self.preprocessing_checks(df, func):
            # split DataFrame into a list of smaller DataFrames
            self.df_split = np.array_split(df, self.partitions, axis=0)
            # create the multiprocessing pool
            pool = Pool(self.cores)
            # process the DataFrame by mapping function to each df across the pool
            df = pd.concat(pool.map(func, self.df_split), axis=0)
            # close down the pool and join
            pool.close()
            pool.join()
            pool.clear()
            return df
