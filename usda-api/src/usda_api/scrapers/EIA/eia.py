import requests
import pandas as pd
import json
class Eia:
    def __init__(self):
        self.endpoint = 'https://api.eia.gov/v2/petroleum/move/wkly/data/?frequency=weekly&data[0]=value&facets[product][]=EPOOXE&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000&api_key=6aG1dQxNbZa4qpcFWbegtT4grgjdUBzgOwyk3C27'
        self.info = None
        self.data_frame = None
    def generate_request(self):
        return requests.get(self.endpoint)
    
    def extract_eia(self):
        request_obj = self.generate_request()
        self.info = request_obj.json()
        return self
    def transform_eia(self):
        if self.info is not None:
            data = self.info['response']['data']
            self.data_frame = pd.DataFrame(data)
        else:
            print('Please extract data')
        return self
        
    def load_eia(self):
        if self.data_frame is not None:
            self.data_frame.to_csv('eia_history.csv')
            print('success')
        else:
            print('Please transform data')
        return self
    
    def get_csv_path():
        return 'eia_history.csv'
        

if __name__ == '__main__':
    eia = EIA()
    eia.extract_eia()
    eia.transform_eia()
    eia.load_eia()