# %%

# packages
import bz2

import numpy as np
import pandas as pd
import json
import glob
import pickle5 as cPickle

# %%


pd.__version__


# %%

# Pickle a file and then compress it into a file with extension
def compressed_pickle(title, data):
    with bz2.BZ2File(title + '.pbz2', 'w') as f:
        cPickle.dump(data, f)


# Load any compressed pickle file
def decompress_pickle(file):
    data = bz2.BZ2File(file, 'rb')
    data = cPickle.load(data)
    return data


dfall = pd.DataFrame()
errorCount = 0
try:
    for filepath in glob.iglob(r'data/mfp-diaries/*'):
        with open(filepath) as f:
            data = json.loads(f.read())
        f.close()

        df = pd.json_normalize(data)

        try:
            df0 = pd.concat({i: pd.DataFrame(x) for i, x in df.pop('summary.total').items()})
            df0.columns = ['total.calories', 'total.carbs', 'total.fat', 'total.protein', 'total.sodium', 'total.sugar']

            try:
                df1 = pd.concat({i: pd.DataFrame(x) for i, x in df.pop('summary.goal').items()})
                df1.columns = ['goal.calories', 'goal.carbs', 'goal.fat', 'goal.protein', 'goal.sodium', 'goal.sugar']

                df2 = df0.join(df1)

                df3 = (df2
                       .reset_index(level=1, drop=True)
                       .join(df)
                       .reset_index(drop=True))

                df4 = df.join(df3.groupby(by=['id', 'date']).sum().reset_index(drop=True))

                dfall = pd.concat([dfall, df4], ignore_index=True)

            except Exception as e:
                errorCount += 1
                print("GOAL column names don't match for: " + filepath)

        except Exception as e:
            errorCount += 1
            print("TOTAL column names don't match for: " + filepath)

except ValueError as error:
    print(str(error))

dfall = dfall.drop(['meals'], axis=1)
print("compressing to pickle")
compressed_pickle("6nutrients", dfall)

print("decompressing from pickle")
dfall = decompress_pickle('6nutrients.pbz2')

print(dfall.info())
print("Number of files without Calories,Carbs,Fat,Protein,Sodium,Sugar: " + str(errorCount))

# %%
