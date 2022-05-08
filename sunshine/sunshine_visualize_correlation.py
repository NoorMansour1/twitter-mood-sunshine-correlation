import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import spearmanr
from datetime import timedelta
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator
sunshine_data = pd.read_csv('sunshine_all.csv', index_col=0)
sentiment_data = pd.read_csv('../data_without_outliers.csv', index_col=0)

sunshine_data.index = pd.to_datetime(sunshine_data.index, utc=True).tz_convert('Europe/Amsterdam')
sentiment_data.index = pd.to_datetime(sentiment_data.index, utc=True).tz_convert('Europe/Amsterdam')

if __name__ == '__main__':
    # Plot just the sunshine data first:
    plt.figure(figsize=(8, 6))
    sns.lineplot(data=sunshine_data['radiation'], label='Hourly radiation', color='yellow')
    sns.lineplot(data=sunshine_data['7d_ma'], label='7-day moving average', color='orange')
    sns.lineplot(data=sunshine_data['30d_ma'], label='30-day moving average', color='red')
    plt.ylabel('Average irradiation ($W/m^2$)')
    plt.savefig('sunshine.pdf')

    plt.figure(figsize=(8, 6))
    sns.lineplot(data=sunshine_data['radiation']['2012-01-01':'2012-01-07'], label='Hourly radiation', color='yellow')
    sns.lineplot(data=sunshine_data['7d_ma']['2012-01-01':'2012-01-07'], label='7-day moving average', color='orange')
    sns.lineplot(data=sunshine_data['30d_ma']['2012-01-01':'2012-01-07'], label='30-day moving average', color='red')
    plt.ylabel('Average irradiation ($W/m^2$)')
    plt.savefig('sunshine2012.pdf')

    # Now we prepare and correlate with the sunshine data:

    sentiment_data['sentiment_7d_ma'] = sentiment_data['sentiment'].rolling(timedelta(days=7)).mean()
    sentiment_data['sentiment_30d_ma'] = sentiment_data['sentiment'].rolling(timedelta(days=30)).mean()

    print(sunshine_data['2014-08-27 20:00':'2014-08-28 0:00'])
    print(sentiment_data.head())

    df = sentiment_data.join(sunshine_data, 'time')
    df.index = df.index.tz_convert('Europe/Amsterdam')
    print(spearmanr(df['radiation'], df['sentiment']))
    print(spearmanr(df['7d_ma'], df['sentiment_7d_ma']))
    print(spearmanr(df['30d_ma'], df['sentiment_30d_ma']))

    plt.figure(figsize=(8, 6))
    sun_plot = sns.lineplot(x=df.index, y=df['30d_ma'], color='r')
    sent_plot = sns.lineplot(x=df.index, y=df['sentiment_30d_ma'], ax=sun_plot.axes.twinx(), color='g')

    sun_plot.set_ylabel('Solar radiation ($W/m^2$)')
    sent_plot.set_ylabel('Sentiment score')

    sun_plot.legend(handles=[
        Line2D([], [], color='r', label='Solar radiation 30 day moving average'),
        Line2D([], [], color='g', label='Sentiment 30 day moving average'),
    ])
    plt.show()


