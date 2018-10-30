import requests
import json
import numpy as np

url = pd.read_csv('data/url.csv')['url'].tolist()
n = len(url)
for i in np.arange(n):
    if i % 100 == 0:
        print(i, 'out of', n)
    req = requests.get(url[i])
    data = req.json()
    with open('data.json','w') as f:
        json.dump(data,f)
    if 'jams' in data.keys():
        jm = pd.DataFrame(eval(str(data['jams'])))
        jm_filtered = jm[jm['city'] == 'Staten Island, NY']
        jm_filtered.to_csv('data_jm/jm_' + str(i) + '.csv', index=False)
