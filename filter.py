import pandas as pd

df = pd.read_csv('sentiment_data.csv.gz', names=['time', 'sentiment', 'num_tweets'], index_col=0)[1:]
print(df.count())

outliers = df[((df.sentiment < df.sentiment.mean() - 3 * df.sentiment.std()) \
	| (df.sentiment > df.sentiment.mean() + 3 * df.sentiment.std()))]

df = df[~((df.sentiment < df.sentiment.mean() - 3 * df.sentiment.std()) \
	| (df.sentiment > df.sentiment.mean() + 3 * df.sentiment.std()))]


print(df.count())
print(outliers.iloc[:,:5])
df.to_csv('data_without_outliers.csv', index_label='time')