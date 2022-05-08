import pandas as pd
import requests
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import seaborn as sns
import time
from pathlib import Path


def get_files(begin_year, end_year, download=True):
    # Returns the filenames of the files in the given year range (including end year), and if download is set to true,
    # downloads the files and saves them in the current directory.
    list_url = "https://api.dataplatform.knmi.nl/open-data/v1/datasets/zonneschijnduur_en_straling/versions/1.0/files"
    api_key = "eyJvcmciOiI1ZTU1NGUxOTI3NGE5NjAwMDEyYTNlYjEiLCJpZCI6ImNjOWE2YjM3ZjVhODQwMDZiMWIzZGIzZDRjYzVjODFiIiwiaCI6Im11cm11cjEyOCJ9 "
    filenames = []

    req = requests.get(
        url=list_url,
        headers={"Authorization": api_key},
        params={
            "maxKeys": 100,
            "startAfterFilename": f'kis_tos_{begin_year}'}
    )

    files = filter(lambda file: int(file["filename"][len('kis_tos_'):len('kis_tos_') + 4]) <= end_year,
                   req.json()["files"])

    for file in files:
        filename = file['filename']
        filenames.append(filename)

        if download:
            file_url = f"https://api.dataplatform.knmi.nl/open-data/v1/datasets/zonneschijnduur_en_straling/versions/1.0/" \
                       f"files/{filename}/url"
            req = requests.get(
                url=file_url,
                headers={"Authorization": api_key}
            )
            while 'error' in req.json().keys():
                time.sleep(10)
                req = requests.get(
                    url=file_url,
                    headers={"Authorization": api_key}
                )
            download_url = req.json()["temporaryDownloadUrl"]

            p = Path(filename)
            p.write_bytes(requests.get(download_url).content)

    return filenames


if __name__ == '__main__':
    download = False
    begin_year = 2010
    end_year = 2016

    filenames = get_files(begin_year, end_year, download)

    table = pd.concat(pd.read_fwf(f, comment='#', header=None,
                                  widths=[21, 20, 48, 20, 20, 20, 20, 20, 20, 20],
                                  usecols=[0, 2, 3, 6],
                                  compression='gzip') for f in filenames)
    table.columns = ['time', 'Station', 'Latitude', 'radiation']
    table.set_index('time', inplace=True)

    table = table[table['Latitude'] > 50]  # Quick way to remove all the Caribbean islands
    table.drop(['Latitude'], axis=1, inplace=True)  # Not needed anymore

    table = table.groupby(['time'], sort=False).mean()  # Get the mean over all stations each interval
    table.index = pd.to_datetime(table.index, format='%Y-%m-%d %H:%M:%S')
    table = table.resample('60T').mean()  # Resample into 1 hour bins
    table['7d_ma'] = table['radiation'].rolling(7*24, min_periods=1).mean()
    table['30d_ma'] = table['radiation'].rolling(30*24, min_periods=1).mean()
    table.interpolate(method='linear', inplace=True)
    print(table)

    months = sns.lineplot(data=table[:24*90])
    months.axes.xaxis.set_major_locator(plt.MaxNLocator(5))
    plt.show()

    plt.figure()
    weeks = sns.lineplot(data=table[24*180:24*194])
    weeks.axes.xaxis.set_major_locator(plt.MaxNLocator(5))
    plt.show()

    plt.figure()
    all = sns.lineplot(data=table[['7d_ma', '30d_ma']])
    all.axes.xaxis.set_major_locator(plt.MaxNLocator(5))
    plt.show()

    table.to_csv('sunshine_all.csv')