import os
import pandas as pd
import numpy as np

class CleaningData:
    def __init__(self, location= "../../Brug/Rauwe data/"):
        """ Loads the data"""
        print(os.listdir(location))
#         self.raw_data = 