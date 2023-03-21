## ------------------------------------------------------------------------------------------------
# Copyright (c) 2023 Snowflake Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.You may obtain 
# a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0
    
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# See the License for the specific language governing permissions andlimitations 
# under the License.
## ------------------------------------------------------------------------------------------------

from snowflake.snowpark.session import Session
import snowflake.snowpark.functions as F
import configparser ,json ,logging
import os ,sys ,subprocess
from snowflake.snowpark.session import Session
from pathlib import Path
import logging ,sys ,os 
import streamlit as st
import pandas as pd
import re

# Import the commonly defined utility scripts using
# dynamic path include
import sys
sys.path.append('src/python/lutils')
import sflk_base as L

logger = logging.getLogger('app_fns')

# -----------------------------------------------------
