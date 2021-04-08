import os
import pandas as pd
import numpy as np
import statsmodels.api as sm
import pdb
from config import *

class Modelling:
    def __init__(self, location=COMBINED_DATA_PATH):
        """ Loads the data
        location: str, (relative) path to the datafolder
        """
        self.data = pd.read_csv(f"{location}combined_table.csv",sep=";",header=[0])
        self.meta = pd.read_csv(f"{location}meta_data.csv",sep=";",header=[0])
    
    def get_difference(self):
        df = self.data.copy()
        for sensor in self.meta['Sensor naam']:
            if self.meta['Element'][self.meta['Sensor naam'] == sensor].iloc[0][:5] == "Boven":
                df[f'To-Tm_{sensor}_diff'] = abs(df[f'To_{sensor}-min'] - df[f'Tm_{sensor}-max'])
            else:
                df[f'To-Tm_{sensor}_diff'] = abs(df[f'To_{sensor}-max'] - df[f'Tm_{sensor}-min'])
        for sensor in self.meta['Sensor naam']:
            df.drop([f'To_{sensor}-mean', f'Tm_{sensor}-mean',f'Td_{sensor}-mean', f'To_{sensor}-min', f'Tm_{sensor}-min',f'Td_{sensor}-min',f'To_{sensor}-max', f'Tm_{sensor}-max',f'Td_{sensor}-max',f'To_{sensor}-std', f'Tm_{sensor}-std',f'Td_{sensor}-std',], axis=1, inplace=True)
        df.drop(['datetime_open','datetime_dicht', 'tijdsduur','dag','maand', 'To_from','To_to','Tm_from','Tm_to','Td_from','Td_to'], axis=1, inplace=True)
        return df
    
    def create_model(self, df):
        y = 1
        return None