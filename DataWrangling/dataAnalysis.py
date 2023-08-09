import os
import fnmatch
import pandas as pd
import json
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde, skew, kurtosis

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



    # Create a histogram
    plt.style.use('dark_background')

    # Create a larger figure
    plt.figure(figsize=(10, 6))

    # Data for the distributions (replace this with your actual data)
    sp500Evol_data = empty_df['sp500Evol']
    ptfEvol_data = empty_df['ptfEvol']

    # Create histograms
    plt.hist(sp500Evol_data, bins=15, alpha=0.5, density=True, label='sp500Evol')
    plt.hist(ptfEvol_data, bins=15, alpha=0.5, density=True, label='ptfEvol')
    plt.xlabel('Evol')
    plt.ylabel('Frequency')
    plt.title('Comparison of sp500Evol and ptfEvol')
    plt.legend()

    # Calculate and add mean lines
    mean_sp500 = sp500Evol_data.mean()
    mean_ptf = ptfEvol_data.mean()
    plt.axvline(mean_sp500, color='red', linestyle='dashed', linewidth=1, label='Mean sp500Evol')
    plt.axvline(mean_ptf, color='green', linestyle='dashed', linewidth=1, label='Mean ptfEvol')

    # Annotate mean values
    plt.text(mean_sp500, 0.05, f'Mean: {mean_sp500:.2f}', color='red', fontsize=10, ha='center')
    plt.text(mean_ptf, 0.05, f'Mean: {mean_ptf:.2f}', color='green', fontsize=10, ha='center')

    # Density plots
    sp500Evol_kde = gaussian_kde(sp500Evol_data)
    ptfEvol_kde = gaussian_kde(ptfEvol_data)

    x_vals = np.linspace(min(min(sp500Evol_data), min(ptfEvol_data)), max(max(sp500Evol_data), max(ptfEvol_data)), 100)
    plt.plot(x_vals, sp500Evol_kde(x_vals), color='red', label='sp500Evol Density')
    plt.plot(x_vals, ptfEvol_kde(x_vals), color='green', label='ptfEvol Density')


    # Calculate and annotate skewness and kurtosis
    sp500_skew = skew(sp500Evol_data)
    sp500_kurt = kurtosis(sp500Evol_data)
    ptf_skew = skew(ptfEvol_data)
    ptf_kurt = kurtosis(ptfEvol_data)

    plt.text(0.23, 0.9, f'Skew (sp500Evol): {sp500_skew:.2f}', color='red', fontsize=10, transform=plt.gca().transAxes, ha='right')
    plt.text(0.23, 0.85, f'Kurtosis (sp500Evol): {sp500_kurt:.2f}', color='red', fontsize=10, transform=plt.gca().transAxes, ha='right')
    plt.text(0.23, 0.8, f'Skew (ptfEvol): {ptf_skew:.2f}', color='green', fontsize=10, transform=plt.gca().transAxes, ha='right')
    plt.text(0.23, 0.75, f'Kurtosis (ptfEvol): {ptf_kurt:.2f}', color='green', fontsize=10, transform=plt.gca().transAxes, ha='right')


    # Show the plot
    plt.legend()
    plt.show()