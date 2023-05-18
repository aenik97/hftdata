import pandas as pd
from tqdm import tqdm
from os import popen


def download_file(row):
    args = ['wget', '-q', '-O', f'{row["symbol"]}_{row["stream"]}_{row["date"]}.log', row['link'], ]
    _ = popen(' '.join(args))
    return None


if __name__ == '__main__':
    # You can use your favorite utility instead wget,
    # or read table other way even just click all links in your browser.
    df = pd.read_csv('../table.csv')

    df = df[df['symbol'] == 'btcusdc']
    df = df[df['stream'] == 'depth20@100ms']
    df = df[df['date'] >= '2023-05-01']

    for row in tqdm(df.iterrows(), total=len(df)):
        download_file(row[1])
