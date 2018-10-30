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
    if 'alerts' in data.keys():
        al = pd.DataFrame(eval(str(data['alerts'])))
        al_filtered = al[al['city'] == 'Staten Island, NY']
        al_filtered.to_csv('data_al/al_' + str(i) + '.csv', index=False)
