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
    if 'irregularities' in data.keys():
        irr = pd.DataFrame(eval(str(data['irregularities']).replace('true', 'True').replace('false', 'False')))
        if 'city' in irr.columns:
            irr_filtered = irr[irr['city'] == 'Staten Island, NY']
            irr_filtered.to_csv('data_irr/irr_' + str(i) + '.csv', index=False)
