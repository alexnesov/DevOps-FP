import pandas as pd
import json
import os
from scipy.stats import ttest_1samp
import plotly.graph_objects as go
import seaborn as sns


def get_all_folders(directory_path):
    # List all entries in the directory
    all_entries = os.listdir(directory_path)
    
    # Filter out only the directories
    folders = [entry for entry in all_entries if os.path.isdir(os.path.join(directory_path, entry))]
    
    return folders


def order_folders_by_date(base_path, folder_names):
    # Sort the folder names by date
    sorted_folders = sorted(folder_names, key=lambda x: (int(x.split('-')[0]), int(x.split('-')[1]), int(x.split('-')[2])))
    
    # Return the full paths
    return [os.path.join(base_path, folder) for folder in sorted_folders]


def find_json_files(folder_path):
    json_files = []

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".json"):
                json_files.append(os.path.join(root, file))

    return json_files



if __name__ == '__main__':

    folders = get_all_folders("/home/nesov/Programmation/DevOps-FP/DataWrangling/output")
    ordered_paths = order_folders_by_date("/home/nesov/Programmation/DevOps-FP/DataWrangling/output", folders)


    empty_dataframe = pd.DataFrame()


    for path in ordered_paths:
        print(path)
        
        json_path = find_json_files(path)[0]
        
        with open(json_path, 'r') as file:
            data = json.load(file)


        selected_data = {
            'n_last_bd': [data['n_last_bd']],
            'return_ptf': [data['return_ptf']],
            'return_sp': [data['return_sp']]
        }


        new_dataframe = pd.DataFrame(selected_data)

        empty_dataframe = pd.concat([empty_dataframe, new_dataframe], ignore_index=True)


        empty_dataframe['diff'] = empty_dataframe['return_ptf'] - empty_dataframe['return_sp']

        print(empty_dataframe)



        print(f"On {len(empty_dataframe)} days of position closed (trade executed), {(empty_dataframe['return_ptf'] < 0).sum()} were negative (winning for a short strategy)")
        factor = (empty_dataframe['return_ptf'] < 0).sum() / len(empty_dataframe)
        print(f"{round(factor,2)}% of the position closed were winning.")
        print(f"Average win (by shorting) {round(-empty_dataframe['diff'].mean(),2)}% for 2 holding days.")



        diff_data = empty_dataframe['diff']
                # Perform the paired t-test
        t_stat, p_value = ttest_1samp(diff_data, 0)

        print(f"T-statistic: {t_stat}")
        print(f"P-value: {p_value}")

        # Check significance
        alpha = 0.05
        if p_value < alpha:
            print("The difference is statistically significant.")
        else:
            print("The difference is not statistically significant.")
