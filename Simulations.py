import pandas as pd
import yfinance as yf
from talib import MA_Type
import talib
import matplotlib.pyplot as plt
import numpy as np
import zlib
from datetime import datetime, timedelta 
from time import gmtime, strftime
import random
from scipy.stats import sem, t
from scipy import mean

import Signal_detection as sd 

today = str(datetime.today().strftime('%Y-%m-%d'))
now = strftime("%H:%M:%S")
now = now.replace(":","-")
currentDirectory = os.getcwd() # Ubuntu

# Parameters
numberOfTickersperTrial = 35
numberOfTrials = 16
Aroonval = 40
short_window =10
long_window = 50
toRandomize = list(pd.read_csv(f"{currentDirectory}/Financial.csv")['Ticker'])
#####

#####
DF_simulation_information = pd.DataFrame(columns=['Trial','Optimal Nb of days','Corresponding Avg return'])
nb_trial = []
list_optimalNbDays = []
list_optimalNbDays_return = []
#####


simulations = {}
 
def generate_simulations():
    for t in list(range(1,numberOfTrials)):
 
        # Selecting 35 different random mid-cap tickers
        len(toRandomize)
        tickers = random.choices(toRandomize, k = numberOfTickersperTrial)
 
        ndays = list(range(1,21)) # number of days after signal for which we want to calculate returns
        dicts = {}
        dicts['n_days'] = ndays
        errors = []
 
        for tick in tickers:
            try:
                try:
                    try:

                        df = yf.download(tick, start = "2016-01-01", end = f"{today}", period = "1d")                       
                        df = sd.SignalDetection(df,tick)                    # Func

                        DFfinalsignal = df[['Date','doubleSignal']]
                        # We get all dates for which we get a double signal, for the time frame considered
                        validDates = df.loc[df['doubleSignal']==1]
 
                        # Initial prices
                        initialPrices = df.loc[df['doubleSignal']==1]['Close']
                        initialPrices_index = df.loc[df['doubleSignal']==1].index
                        initialPrices = np.array(initialPrices.tolist())
 
                        # Calculate the date between day signal was given and n_th day after
                        # Prices after n_day
                        n_day = []
                        evolution = []
                        for n in ndays:
                            nextPrices_index = df.loc[df['doubleSignal']==1].index + n
                            nextPrices = df[df.index.isin(nextPrices_index)]['Close']
                            nextPrices = np.array(nextPrices.tolist())
                            try:
                                mean_evolution = ((nextPrices-initialPrices)/initialPrices).mean()
                                evolution.append(mean_evolution)
                                n_day.append(n)
                            except ValueError:
                                initialPrices = initialPrices[:-1]
                                mean_evolution = ((nextPrices-initialPrices)/initialPrices).mean()
                                evolution.append(mean_evolution)
                                n_day.append(n)
 
                        dicts[f'{tick}'] = evolution
                    except KeyError:
                        pass
                except TypeError:
                    errors.append(tick)
            except ValueError:
                try:
                    error.apend(tick)
                except NameError:
                    pass
                 
        means = pd.DataFrame.from_dict(dicts).set_index("n_days")
        means = means.mean(axis = 1) 
        day1 = round(float(means.iloc[[0]]),3)
        day10 = round(float(means.iloc[[9]]),3)
        day15 = round(float(means.iloc[[14]]),3)
        day20 = round(float(means.iloc[[19]]),3)
        optimalNbDays = means.idxmax()
        optimalNbDays_return = round(means.loc[optimalNbDays],3)
        pcnt = round(optimalNbDays_return*100,2)
        sentence = "Average return after"
 
        print( f"{sentence} 1 day: {day1}\n \
        {sentence} 10 days: {day10}\n \
        {sentence} 15 days: {day15}\n \
        {sentence} 20 days: {day20}\n \n \
        Optimal number of holding days is: {optimalNbDays} \n \
        Corresponding average return is: {optimalNbDays_return} ({pcnt}%)" )
 
        fig = plt.figure(figsize=(15,8))
        ax = plt.axes()
        ax.plot(means)
        ax.grid(color='grey', linestyle='-', linewidth=0.25, alpha=0.9)
        ax.spines['top'].set_visible(False)
 
        ax.spines['right'].set_visible(False)
        fig.suptitle(f'Optimal number of holding days is: {optimalNbDays} days \n \
        Corresponding average return is: {pcnt}%',
        fontsize=12)
        plt.xlabel('N_days', fontsize=12)
        plt.ylabel('Return', fontsize=12)
        plt.savefig(f'Try {t}.png')
 
        simulations[f'Try {t}'] = means
         
        list_optimalNbDays.append(optimalNbDays)
        list_optimalNbDays_return.append(optimalNbDays_return)
        nb_trial.append(t)
 
        print(f"Simulation{t}/15 finished.")
 
 
 
if __name__ == "__main__":

    generate_simulations()
    simuationsDF = pd.DataFrame.from_dict(simulations)
    DF_simulation_information['Trial'] = nb_trial
    DF_simulation_information['Optimal Nb of days'] = list_optimalNbDays
    DF_simulation_information['Corresponding Avg return'] = list_optimalNbDays_return
    stats_std = stats.stdev(DF_simulation_information['Corresponding Avg return'])
    stats_mean = stats.mean(DF_simulation_information['Corresponding Avg return'])
    data = DF_simulation_information['Corresponding Avg return']
