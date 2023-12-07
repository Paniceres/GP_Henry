import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import seaborn as sns
import os.path
from funcs import read_config, get_groups, pull_clean, get_kpi2_respuestas, get_kpi3_retencion, get_kpi4_influencia

# Obtener la ruta del directorio del script actual
route = os.path.dirname(__file__)

#Data Pull and Functions
data_frames = pull_clean() 

state = data_frames.get('1_states.parquet')
business_google = data_frames.get('5_business_google.parquet')


groups = get_groups(business_google)

print(groups.columns) 