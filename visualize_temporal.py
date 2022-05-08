import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta
import datetime
from scipy import stats

def set_size(w,h, ax=None):
    """ w, h: width, height in inches """
    if not ax: ax=plt.gca()
    l = ax.figure.subplotpars.left
    r = ax.figure.subplotpars.right
    t = ax.figure.subplotpars.top
    b = ax.figure.subplotpars.bottom
    figw = float(w)/(r-l)
    figh = float(h)/(t-b)
    ax.figure.set_size_inches(figw, figh)

trns = lambda df: (df.index, df['sentiment'])

df = pd.read_csv('sentiment_data', names=['time', 'sentiment', 'num_tweets'], index_col=0)
df.index = pd.to_datetime(df.index, utc=True).tz_convert('Europe/Amsterdam')

# filter data
x, y = df.index, df['sentiment']
mean = np.mean(y)
var = np.std(y)
min, max = mean - 3 * var, mean + 3 * var
scatter_color = ['red' if sent < min or sent > max else 'green' for sent in y]
df = df[~((df['sentiment'] < min) | (df['sentiment'] > max))]
x_out, y_out = df.index, df['sentiment']

# create aggregation data
hour_data = df.groupby(df.index.hour).mean('sentiment')
x_hour, y_hour = trns(hour_data)

week_data = df.groupby(df.index.week).mean('sentiment')
x_week, y_week = trns(week_data)


month_data = df.groupby(df.index.month).mean('sentiment')
x_month, y_month = trns(month_data)

day_of_week_data = df.groupby(df.index.weekday).mean('sentiment')
x_day_week, y_day_week = trns(day_of_week_data)

day_of_month_data = df.groupby(df.index.day).mean('sentiment')
x_day_mont, y_day_month = trns(day_of_month_data)

# uniform dist test
print(stats.kstest(y_hour, stats.uniform(loc=0.0, scale=100.0).cdf))
print(stats.kstest(y_week, stats.uniform(loc=0.0, scale=100.0).cdf))
print(stats.kstest(y_month, stats.uniform(loc=0.0, scale=100.0).cdf))
print(stats.kstest(y_day_month, stats.uniform(loc=0.0, scale=100.0).cdf))
print(stats.kstest(y_day_month[0:5], stats.uniform(loc=0.0, scale=100.0).cdf))


# create plots
fig1, axes = plt.subplots(5)
fig2, ax1 = plt.subplots(1)
fig3, ax2 = plt.subplots(1)

# aggr plots
axes[0].bar(x_hour,y_hour, color='tab:green')
axes[0].plot(x_hour, [np.mean(y_hour)]*len(x_hour), color='red')
axes[1].bar(x_day_week,y_day_week, color='tab:green')
axes[1].plot(x_day_week, [np.mean(y_day_week)]*len(x_day_week), color='red')
axes[2].bar(x_day_mont,y_day_month, color='tab:green')
axes[2].plot(x_day_mont, [np.mean(y_day_month)]*len(x_day_mont), color='red')
axes[3].bar(x_week,y_week, color='tab:green')
axes[3].plot(x_week, [np.mean(y_week)]*len(x_week), color='red')
axes[4].bar(x_month,y_month, color='tab:green', label='Sentiment')
axes[4].plot(x_month, [np.mean(y_month)]*len(x_month), color='red', label='Mean sentiment')



# axes[0].plot(x_hour,y_hour, c ='green')
# axes[1].plot(x_week,y_week, c ='green')
# axes[2].plot(x_month,y_month, c ='green')

for (a, title) in zip(axes,['(a)','(b)','(c)','(d)','(e)']):
	a.set_ylabel('Sentiment')
	a.set_ylim(0.025,0.05)
	a.set_title(title, loc='left')


axes[0].set_xlabel('Hour of day')
axes[0].set_xticks([0, 4, 8, 12, 16, 20, 23])

axes[1].set_xlabel('Day of week')
axes[1].set_xticks([0,1,2,3,4,5,6])
axes[1].set_xticklabels(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])

axes[2].set_xlabel('Day of month')
axes[2].set_xticks(np.arange(1, 32))

axes[3].set_xlabel('Week of year')
axes[3].set_xticks([1, 6, 12, 18, 24, 30, 36, 42, 48, 53])
axes[4].set_xlabel('Month of year')
axes[4].set_xticks([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
# axes[2].set_xticklabels(['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'])

handles, labels = axes[4].get_legend_handles_labels()
fig1.legend(handles, labels, loc='upper center', prop={'size': 10})

fig1.set_size_inches(10, 10)
fig1.tight_layout(pad=1.0)

fig1.savefig('aggr_data.pdf')


# filter plot
ax1.scatter(x_out,y_out,s=30, c='tab:red',label='Outlier')
ax1.scatter(x,y,s=2, c='tab:blue', label='Residuum')
ax1.set_xlabel('Time', fontsize=20)
ax1.set_ylabel('Sentiment', fontsize=20)
idx = np.linspace(0,len(x) - 1,5).astype(int)
print(x)
ax1.set_xticks(x[idx])
ax1.tick_params(labelsize=20)
ax1.legend(loc=2, prop={'size': 20})

# fig2.set_size_inches(18.5, 10.5)
fig2.tight_layout(pad=3.0)
# ax.set_xticks(x[::len(x)-1])

# plt.show()
fig2.savefig('filtered_data.pdf')

# moving averages
y_7ma = df['sentiment'].rolling(timedelta(days=7)).mean()
y_30ma = df['sentiment'].rolling(timedelta(days=30)).mean()

ax2.plot(df.index, y_7ma, c='limegreen', label='7-day moving average')
ax2.plot(df.index, y_30ma, c='darkgreen', label='30-day moving average')
ax2.set_xlabel('Time')
ax2.set_ylabel('Sentiment')
fig3.legend(loc=2, prop={'size': 10})
fig3.savefig('sent_ma.pdf')