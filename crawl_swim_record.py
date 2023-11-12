import argparse

import bs4
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import pandas as pd
import requests

COLUMNS = ["Event", "Time", "Date", "Meet", "Source"]


def extract_result_from_swimming_rank(user_id):
    import warnings
    warnings.filterwarnings("ignore")
    
    r = requests.get(f"https://www.swimmingrank.com/ne/strokes/strokes_ne/{user_id}_meets.html")
    if r.status_code != 200:
        print(f"Failed: {user_id}")
        return
    
    soup = BeautifulSoup(r.text, "html.parser")
    
    event_menu = soup.find_all(id="event_menu")[0].find_all(id="navbar")[0]
    result_dfs = []
    for x in event_menu:
        if isinstance(x, bs4.element.Tag) and x.text != "Meets":
            result_url = list(x.children)[0].attrs["href"]
            r = requests.get(result_url)
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find(lambda tag: tag.name == "h2" and "Best Time" in tag.text).find_next("table")
            dfs = pd.read_html(str(table))
            assert len(dfs) == 1
            df = dfs[0]
            df["Source"] = "Swimming Rank"
            df = df[COLUMNS]
            result_dfs.append(df)
            
    return pd.concat(result_dfs, axis=0)


def rename_swimcloud_event(swimcloud_event):
    distance, unit, stroke = swimcloud_event.split()
    if unit == "Y":
        unit = "Yd"
    elif unit == "L":
        unit = "M"
    else:
        raise ValueError(f"Unknown unit: {unit}")
    return " ".join([distance, unit, stroke])


def extract_result_from_swimcloud(user_id):
    import warnings
    warnings.filterwarnings("ignore")
    
    r = requests.get(f"https://www.swimcloud.com/swimmer/{user_id}/")
    if r.status_code != 200:
        print(f"Failed: {user_id}")
        return
        
    soup = BeautifulSoup(r.text, "html.parser")
    table_container = soup.find_all(id="js-swimmer-profile-times-container")[0]
    data = []
    for row in list(table_container.find("tbody").children):
        if row and not isinstance(row, str):
            row = [x.strip() for x in row.text.split("\n") if x.strip() and (len(x.strip()) != 1)]
            data.append(row)
            
    df = pd.DataFrame(data, columns=["Event", "Time", "Meet", "Date"])
    df["Source"] = "Swimcloud"
    df["Event"] = df["Event"].apply(rename_swimcloud_event)
    
    return df[COLUMNS]


def time_str_to_seconds(time_str):
    tokens = time_str.split(":")
    if len(tokens) == 1:
        return float(tokens[0])
    
    return int(tokens[0]) * 60 + float(tokens[1])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True, type=str)
    parser.add_argument("-o", "--output", type=str, default="output.xlsx")
 
    return parser.parse_args()   


def main():
    args = parse_args()
    
    swim_id_df = pd.read_excel(args.input)
    
    result_dfs = []
    
    # Extract results from Swimming Rank
    names = []
    swimming_rank_ids = []
    for name, _, ids in swim_id_df.itertuples(index=False):
        for swimming_rank_id in ids.split(";"):
            names.append(name)
            swimming_rank_ids.append(swimming_rank_id)
            
    dfs = Parallel(n_jobs=len(swimming_rank_ids))(
        delayed(extract_result_from_swimming_rank)(x)
        for x in swimming_rank_ids)
    for name, df in zip(names, dfs):
        df["name"] = name
    result_dfs.extend(dfs)
    
    # Extract results from Swimcloud
    names = []
    swimcloud_ids = []
    for name, swimcloud_id, _ in swim_id_df.itertuples(index=False):
        names.append(name)
        swimcloud_ids.append(int(swimcloud_id))
            
    dfs = Parallel(n_jobs=len(swimcloud_ids))(
        delayed(extract_result_from_swimcloud)(x)
        for x in swimcloud_ids)
    for name, df in zip(names, dfs):
        df["name"] = name
    result_dfs.extend(dfs)
 
    # Merge results from different sources       
    result_df = pd.concat(result_dfs, axis=0)
    result_df["Time"] = result_df["Time"].astype(str)
    result_df["Time (seconds)"] = result_df["Time"].apply(time_str_to_seconds)
    result_df["source_order"] = result_df["Source"].apply(lambda x: {
        "Swimming Rank": 0,
        "Swimcloud": 1
    }.get(x, 1000))
    
    with pd.ExcelWriter(args.output) as writer:
        for name, swimmer_all_records_df in result_df.groupby("name"):
            records = []
            for event, sub_df in swimmer_all_records_df.groupby("Event"):
                sub_df = sub_df.sort_values(
                    by=["source_order", "Time (seconds)"], ascending=True)
                records.append(sub_df.iloc[:1])
                
            swimmer_df = pd.concat(records)
            
            new_columns = ["Distance", "Unit", "Stroke"]
            swimmer_df[new_columns] = swimmer_df["Event"].str.split(" ", expand=True)
            swimmer_df["Distance"] = swimmer_df["Distance"].astype(int)
            
            (
                swimmer_df[new_columns + COLUMNS]
                .sort_values(by=["Stroke", "Distance", "Unit"])
                .to_excel(writer, index=False, sheet_name=name)
            )


if __name__ == "__main__":
    main()