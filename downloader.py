import os
import re
import sys
import concurrent.futures as fs
import threading


MAX_RETRIES   = 10
MAX_WORKERS = 16
TO_DIR = ""

NUM_URLS = 0
NUM_DONE = 0
MUTEX_NUM = threading.Lock()

API_KEY = ##"add your API_KEY here" ##obtained from your own EarthData account


def getRemoteSize(URL):
    for i in range(MAX_RETRIES):
        text = os.popen(f'curl -sIL --max-time 60 --header "Authorization: Bearer {API_KEY}" {URL}').read()
        elements = re.findall(r"\ncontent-length: (\d+)\n", text)
        if len(elements) == 2:
            size = int(elements[1])
            break
    else:
        size = 0
        print("*"*720)
        print(elements)
        print(text)
        exit()
    return size

def downloadURL(URL, to_path):
    os.system(f'curl -sL --max-time 800 --header "Authorization: Bearer {API_KEY}" -o {to_path} {URL}')


def getLocalSize(path):
    if os.path.exists(path) and not os.path.isdir(path):
        return os.path.getsize(path)
    else:
        return 0

def runTask(URL):
    file_size = getRemoteSize(URL)
    file_name = URL.split("/")[-1]
    for i in range(MAX_RETRIES):
        if getLocalSize(os.path.join(TO_DIR, file_name)) == file_size:
            with MUTEX_NUM:
                global NUM_DONE
                NUM_DONE += 1
                print(f"Finished: {NUM_DONE}/{NUM_URLS}, {file_name}")
            break
        else:
            downloadURL(URL, os.path.join(TO_DIR, file_name))
    else:
        print("Failed", URL, file_size)

if __name__ == "__main__":
    file_path = sys.argv[1]
    TO_DIR    = sys.argv[2]
    MAX_WORKERS = int(sys.argv[3])
    
    with open(file_path, 'r') as f:
        URLs = [line.strip() for line in f.readlines()]
    if "fetch_urls <<'EDSCEOF'" in URLs:
        idx = URLs.index("fetch_urls <<'EDSCEOF'")
        URLs = URLs[idx+1:-1]
    NUM_URLS = len(URLs)
    
    if not os.path.exists(TO_DIR):
        os.system(f"mkdir -p {TO_DIR}")

    with fs.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        exe.map(runTask, URLs)