import os
import pandas as pd
import numpy as np
import pdb
from config import *

class PrepareDataset:
    def __init__(self, location=PATH, selection='202006'):
        """ Loads the data
        location: str, (relative) path to the datafolder
        selection: str, part of the datafile that indicates the month
        """
        if selection.lower() == "all":
            self.raw_data = pd.DataFrame()
            for file in os.listdir(location):
                if file.endswith("-opzetstukken.csv"):
                    data = pd.read_csv(f"{location}{file}",sep=";")
                    self.raw_data = self.raw_data.append(data)
        else:
            self.raw_data = pd.read_csv(f"{location}{selection}-opzetstukken.csv",sep=";")
            
        self.all_meteo_data = pd.read_csv(f"{location}{METEO}",sep=";",header=[0],skiprows=[1], dtype='unicode')
    
    def preprocessing_pipeline(self):
        opz = self.format_data(self.raw_data)
        cld = self.clean_data(opz)
        [start, end] = self._get_start_and_enddate(cld)
        meteo = self.filter_meteo_data(start, end)
        meteo = self.clean_meteo_data(meteo)
        momenten = self.determine_moments(cld)
        return {'momenten': momenten, 'meteo': meteo}
        
    def format_data(self, raw_data):
        """ Takes the raw data and changes it to the right formats
        """
        opz = raw_data.copy()
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
#         pdb.set_trace()
        mask = (opz['Opzetstuk Noord (°)']<-1) | (opz['Opzetstuk Noord (°)']>100)
        opz = opz.drop(opz.loc[mask].index)
        opz['open'] = opz["Opzetstuk Noord (°)"].apply(lambda x: 1 if x < 80 else 0)
        #Deze klopt niet. We hebben het moment nodig van opengaan en het moment van dichtgaat. Moment van openen is: wanneer de verandering van de aantal graden >1 graad is. Moment van sluiten is de laatste verandering totdat het niet meer veranderd. Zie ook code van Pieter in C#.
        opz['diff'] = opz['open'].diff()
        beweegt=opz[opz['diff']!=0]
        return beweegt

    def determine_moments(self, beweegt):
        """ Bepalen van moment van openen en moment van sluiten.
        """
        momenten=beweegt.copy()
        momenten['timedelta'] = momenten['datetime'].diff()
        momenten.fillna(pd.Timedelta(seconds=0), inplace=True)
        momenten['timedelta_secs'] = momenten['timedelta'].dt.total_seconds()
        return momenten
    
    def _get_start_and_enddate(self, df):
        start = df.datetime.min()
        end = df.datetime.max()
        return [start, end]
        
    def filter_meteo_data(self, startdate, enddate):
        """Filters the meteodataset to the timewindow determined by startdate and enddate
        Parameters:
        -----------
        startdate: str, date from where to filter the meteo data
        enddate: str, date to where to filter the meteo data
        
        Returns:
        --------
        meteodata: pd.DataFrame, the filtered meteo data
        """
        self.all_meteo_data.columns.values[0]='Datum-tijd'
        self.all_meteo_data['datetime']=pd.to_datetime(self.all_meteo_data['Datum-tijd'], format='%Y-%m-%dT%H:%M:%SZ')
        self.all_meteo_data.drop(['Datum-tijd'],axis=1, inplace=True)
        mask = (self.all_meteo_data['datetime'] > startdate) & (self.all_meteo_data['datetime'] <= enddate)
        meteodata = self.all_meteo_data.loc[mask].copy()
        meteodata.set_index('datetime',inplace=True)
        return meteodata
    
    def clean_meteo_data(self, df):
        """Cleans the meteodataset by imputing missing data, transforming to the right format and resampling to 10 minutes
        
        Parameters:
        -----------
        df: pd.DataFrame, the filtered meteo data
        
        Returns:
        --------
        df: pd.DataFrame, cleaned and resampled dataset
        """
        for col in df.columns:
            df[col] = df[col].str.replace(',', '.').astype("float")
#         df_nan = df[df.isna().any(axis=1)]
#         print("Check Nans:",df_nan.shape[0])
        df=df.fillna(method='ffill')
#         df_nan = df[df.isna().any(axis=1)]
#         print("Check Nans:",df_nan.shape[0])
#         print("shape selected sensor data:",df.shape)
        df=df.dropna()
        df=df.resample("10T").mean()
        df=df.reset_index()
        df['dag']=df['datetime'].dt.day
        return df




        # Uiteindelijke output is een dataframe met daarin een regel per combinatie open en closed. Daarin staat een kolom open tijd, sluit tijd, tijd ertussen
        
        # Dan moet er nog een stuk komen (misschien andere class) met inladen strain. daarvan nodig: strain op moment van openen, strain op moment van sluiten, min (beneden hangende sensor)/max (Hooghangende sensor) strain in de periode tussen openen en sluiten.
