import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote


csv_file_path = r'C:\Users\kajal\IdeaProjects\MiniPrjPython\Data\more1_data.csv'
df = pd.read_csv(csv_file_path)
print(df)

password = quote('WelcomeItc@2022')
engine = create_engine(
    'postgresql://consultants:' + password + '@ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb')
print('connected')
df.to_sql('emp_info', engine, if_exists='append', index=False)
print('loaded')
