import requests
import pandas as pd
from bs4 import BeautifulSoup
import lxml

##helper functions
MAXRES_STR = "&maxresults="
MARKER_STR = "&marker="

URL = "https://modawazedata.blob.core.windows.net/raw-upload?restype=container&comp=list&timeout=60"

#build url with max results and next marker
def build_string(max_res, next_marker, url=URL, maxres_str=MAXRES_STR, marker_str=MARKER_STR):
    max_str = maxres_str + str(max_res)
    marker = marker_str + next_marker
    return url + max_str + marker

#make request and soupify
def get_request(url):
    r = requests.get(url)
    c = r.content
    soup = BeautifulSoup(c,'lxml')
    return soup

#generate list of blob names
def make_list_of_blobnames(blob_vals):
    blob_names = []
    for n in blob_vals:
        blob_names.append(n.text)
    return blob_names

#get request and return list of filenames and next marker
def get_filenames_and_marker(soup):
    soup_name = soup.find_all('name')
    
    #generate list of blobnames
    filename_list = make_list_of_blobnames(soup_name)
    
    #get next marker
    marker = soup.find_all('nextmarker')[0].text
    
    return filename_list, marker

#use recursive method with max_results to get all filenames in container 
def get_next_marker_vals(rec_cnt, marker, url, all_blobs):
    if rec_cnt == 0:
        return all_blobs
    else:
        new_url = build_string(rec_cnt, marker, url=url, maxres_str=MAXRES_STR, marker_str=MARKER_STR)
        #print("new URL in recursion ", new_url)
        soup = get_request(new_url)
        blob_list, marker = get_filenames_and_marker(soup)
        all_blobs += blob_list
        print('number of blob filenames ', len(all_blobs))
        rec_cnt = rec_cnt-5000
        print('record count ', rec_cnt)
        
        return get_next_marker_vals(rec_cnt, marker, url, all_blobs)

NUM_RECORDS = 100000 #increments of 5000

url = "https://modawazedata.blob.core.windows.net/raw-upload?restype=container&comp=list&timeout=60"

#create list object for all blobs names
all_blobs = []

#make first call to url and get the nextmarker value for subsequent calls
s = get_request(url)
blob_list, mark = get_filenames_and_marker(s)
all_blobs += blob_list # add blob names to list

blobnames = get_next_marker_vals(NUM_RECORDS, mark, url, all_blobs)

base_url = "https://modawazedata.blob.core.windows.net/raw-upload/"
blobnames = list(set(blobnames))

file_url_list = []
for b in blobnames:
    file_url_list.append(base_url + b)
file_url_list = sorted(file_url_list)

df = pd.DataFrame({'url': file_url_list})
df['time'] = df['url'].str[71:90]
df.to_csv('url.csv', index=False)