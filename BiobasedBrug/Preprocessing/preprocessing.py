import os
import pandas as pd
import numpy as np
from config import *

class CleaningData:
    def __init__(self, location=PATH, selection='202006'):
        """ Loads the data
        location: str, (relative) path to the datafolder
        selection: str, part of the datafile that indicates the month
        """
        self.raw_data = pd.read_csv(f"{location}{selection}-opzetstukken.csv",sep=";")
    
    def preprocessing_pipeline(self):
        opz = self.format_data(self.raw_data)
        cld = self.clean_data(opz)
        return cld
        
    def format_data(self, raw_data):
        """ Takes the raw data and changes it to the right formats
        """
        opz = raw_data
        opz['datetime'] = pd.to_datetime(opz['Datum-tijd'], format='%Y-%m-%dT%H:%M:%SZ')
        opz.drop(['Datum-tijd'],axis=1, inplace=True)
        opz['dag']=opz['datetime'].dt.day
        opz['tijd'] = opz['datetime'].dt.time
        #voeg open/dicht data toe en bepaal momenten waarop dit wisselt
        opz['Opzetstuk Noord (°)'] = opz['Opzetstuk Noord (°)'].str.replace(',', '.').astype(float)
        opz['Opzetstuk Zuid (°)'] = opz['Opzetstuk Zuid (°)'].str.replace(',', '.').astype(float)
        opz['Opzetstuk Noord (°)'].fillna(opz['Opzetstuk Zuid (°)'], inplace=True)
        opz['Opzetstuk Zuid (°)'].fillna(opz['Opzetstuk Noord (°)'], inplace=True)
        return opz
    
    def clean_data(self, opz):
        """ Cleans data by deleting mistakes
            Hier mist nog wat: er word nog niks gedaan met nans
        """
        mask = (opz['Opzetstuk Noord (°)']<-1) | (opz['Opzetstuk Noord (°)']>100)
        opz = opz.drop(opz.loc[mask].index)
        opz['open'] = opz["Opzetstuk Noord (°)"].apply(lambda x: 1 if x < 80 else 0)
        #Deze klopt niet. We hebben het moment nodig van opengaan en het moment van dichtgaat. Moment van openen is: wanneer de verandering van de aantal graden >1 graad is. Moment van sluiten is de laatste verandering totdat het niet meer veranderd. Zie ook code van Pieter in C#.
        opz['diff'] = opz['open'].diff()
        beweegt=opz[opz['diff']!=0]
        return beweegt

    def determine_moments(self, beweegt):
        """ Bepalen van moment van openen en moment van sluiten. Nodig 
        """
        momenten=beweegt.copy()
        momenten['timedelta'] = momenten['datetime'].diff()
        momenten.fillna(pd.Timedelta(seconds=0), inplace=True)
        momenten['timedelta_secs'] = momenten['timedelta'].dt.total_seconds()
        return momenten
        
        # Uiteindelijke output is een dataframe met daarin een regel per combinatie open en closed. Daarin staat een kolom open tijd, sluit tijd, tijd ertussen
        
        # Dan moet er nog een stuk komen (misschien andere class) met inladen strain. daarvan nodig: strain op moment van openen, strain op moment van sluiten, min (beneden hangende sensor)/max (Hooghangende sensor) strain in de periode tussen openen en sluiten.
