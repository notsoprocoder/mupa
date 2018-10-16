### Parallel Pandas

The aim of this package is provide a tool kit for Pandas, in particular for processing large DataFrames containing strings.

For installation ensure that requirements.txt has been installed prior to import. Run:
    pip install -r requirements.txt.

'pip install -r requirements.txt'

The mupa directory needs to sit within your project and imports as a python package.

Mupa will soon implements 3 tools to circumnavigate the lack of vectorisation of Pandas DataFrames.
These are:
    - Multi-Processing
    - Multi-Threading
    - Cython Fast Loops

So far the multi-processing class has been built and I am in the process of building several utility functions and writing an article discussing processing improvements. Then I will look at Threading and Fast Loops.

## Multi-Processing
This uses numpy's array_split to split the DataFrame along axis 0, before creating a Pool of the processors cores and using the pool the map the function to the DataFrame across the cores.

The underlying code was taken from maxpowerwastaken; the logic and performance behind these are explored in a couple of awesome articles:
https://maxpowerwastaken.github.io/blog/multiprocessing-with-pandas/
http://chriskiehl.com/article/parallelism-in-one-line/


The pandas_mp facility needs to be initialised as a class.
When initialising the class you have the option to assign the number of partitions and the number of cores.
If unspecified these will default to the number of cores in the machines.
parallel_apply will process your function; it takes two arguments the DataFrame and a function.
The function will need to apply take and return a Pandas DataFrame.

Code example:
import pandas as pd
import parallel_pandas.multiprocessing as pmp

df = pd.read_csv('some_data.csv')

def foo(df):
    df['foo'] = 'foo'
    return df

pmp = pmp(num_parts = 8, num_cores = 2)
df = pmp.parallel_apply(df, foo)


## TO DO:
### complete unit-tests
### Testing can be done via PyTest - run in terminal: pytest filename.py
### Build Multi Processing facilitator closure func
### Assess performance improvements
### Build Multi Threading
### Build Fast Loops
### Collect data on performance - measure each functions performance on dataframes of increasing sizes.