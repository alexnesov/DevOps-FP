import os
import fnmatch
import pandas as pd
import json
import matplotlib.pyplot as plt


def find_folders_with_name(root_dir, target_name):
    matching_folders = []

    for root, dirs, files in os.walk(root_dir):
        for dir_name in dirs:
            if fnmatch.fnmatch(dir_name, f"*{target_name}*"):
                matching_folders.append(os.path.join(root, dir_name))

    return matching_folders


def find_json_files(root_dir):
    json_files = []

    for root, dirs, files in os.walk(root_dir):
        for file_name in files:
            if fnmatch.fnmatch(file_name, "*.json"):
                json_files.append(os.path.join(root, file_name))

    return json_files


if __name__ == '__main__':
    root_directory = "/home/nesov/Programmation/DevOps-FP/DataWrangling/output"
    target_name = "2023"

    matching_folders = find_folders_with_name(root_directory, target_name)

    # Create an empty DataFrame with specified columns
    columns = ['sp500Evol', 'ptfEvol']
    empty_df = pd.DataFrame(columns=columns)

    for folder in matching_folders:
        # Load JSON data from the file
        j_file = find_json_files(folder)[0]
        with open(j_file, "r") as json_file:
            json_data = json.load(json_file)
            ret_ptf = json_data['return_ptf']
            ret_sp  = json_data['return_sp']
            empty_df = empty_df._append({'sp500Evol': ret_sp, 'ptfEvol': ret_ptf}, ignore_index=True)

        

    print(empty_df)
    print(empty_df['ptfEvol'].mean())


    # Create a histogram
    plt.style.use('dark_background')
    # Create a larger figure
    plt.figure(figsize=(10, 6))

    # Create a histogram
    plt.hist(empty_df['sp500Evol'], bins=15, alpha=0.5, label='sp500Evol')
    plt.hist(empty_df['ptfEvol'], bins=15, alpha=0.5, label='ptfEvol')
    plt.xlabel('Evol')
    plt.ylabel('Frequency')
    plt.title('Comparison of sp500Evol and ptfEvol')
    plt.legend()

    # Calculate and add mean lines
    mean_sp500 = empty_df['sp500Evol'].mean()
    mean_ptf = empty_df['ptfEvol'].mean()
    plt.axvline(mean_sp500, color='red', linestyle='dashed', linewidth=1, label='Mean sp500Evol')
    plt.axvline(mean_ptf, color='green', linestyle='dashed', linewidth=1, label='Mean ptfEvol')
    
    # Annotate mean values
    plt.text(mean_sp500, 5, f'Mean: {mean_sp500:.2f}', color='red', fontsize=10, ha='center')
    plt.text(mean_ptf, 5, f'Mean: {mean_ptf:.2f}', color='green', fontsize=10, ha='center')

    # Show the plot
    plt.legend()
    plt.show()