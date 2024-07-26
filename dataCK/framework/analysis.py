import os
import json
import pandas as pd
import re
import numpy as np
from utils.util import concatCSV, get_file


class Analysis:
    def __init__(self, filePath):
        self.filePath = filePath
    
    def load_csv(self):
        df = concatCS(self.filePath)
        return df
        
    def pre_deal(df):
        pass
    
    def analysis():
        pass
