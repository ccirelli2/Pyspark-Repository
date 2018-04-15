import pandas as pd



df = pd.DataFrame({})

df['Array1'] = [1,2,3,4,5]
df['Array2'] = [3,5,4,3,2]

print(df.plot.bar())



