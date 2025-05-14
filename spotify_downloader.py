import aiohttp
import asyncio
import zstandard as zstd
import gzip
from datetime import datetime
import csv
import io
import os
import sys
import random
from urllib.parse import unquote, urlparse

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def get_metadata(track_url, session):
    """
    Fetches metadata from the /api/get-metadata endpoint and returns the track URLs.
    """
    url = "https://spotymate.com/api/get-metadata"
    
    # JSON payload for the metadata request
    data = {
        "url": track_url
    }

    # Headers to mimic a real browser request
    headers = {
        ":authority": "spotymate.com",
        ":method": "POST",
        ":path": "/api/get-metadata",
        ":scheme": "https",
        "accept": "*/*",
        "accept-encoding": "identity",
        "accept-language": "el-GR,el;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "cookie": "_ga=GA1.1.644048383.1741860429; _ga_7TT8VXDG7S=GS1.1.1741860429.1.1.1741862325.0.0.0",
        "origin": "https://spotymate.com",
        "pragma": "no-cache",
        "referer": "https://spotymate.com/",
        "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    j = 0
    while(j < 3):
        j+=1
        try:
            # Sending POST request for metadata
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 200:
                    # Check if the response has zstd compression
                    content_encoding = response.headers.get('Content-Encoding', '')

                    compressed_data = await response.read()

                    if 'zstd' in content_encoding:
                        try:
                            # Decompress the response using zstd
                            dctx = zstd.ZstdDecompressor()
                            decompressed_data = dctx.decompress(compressed_data)
                            response_json = decompressed_data.decode('utf-8')  # Decode the decompressed data into text
                            print("Decompression successful!")
                        except zstd.ZstdError as e:
                            print(f"Error decompressing with zstd: {e}")
                            print(f"[{get_timestamp()}] Attempting fallback with gzip...")
                            try:
                                # Attempting gzip decompression
                                decompressed_data = gzip.decompress(compressed_data)
                                response_json = decompressed_data.decode('utf-8')
                                print(f"[{get_timestamp()}] Gzip decompression successful!")
                            except Exception as ex:
                                print(f"Error decompressing with gzip: {ex}")
                                
                    else:
                        # If not compressed, simply read the JSON
                        try:
                            response_json = await response.json()
                        except Exception as e:
                            print(f"Error reading JSON: {e}")
                            

                    # Try to load it as a JSON object
                    try:
                        data = response_json if isinstance(response_json, dict) else eval(response_json)
                        track_urls = [track['url'] for track in data['apiResponse']['data']]
                        #print(f"Got spotify link URLs: {track_urls}")
                        return track_urls
                    except Exception as e:
                        print(f"Error parsing JSON: {e}")
                        
                else:
                    print(f"Failed to fetch metadata, Status Code: {response.status}")

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            print(f"[{get_timestamp()}] Waiting before retrying...")
            await asyncio.sleep(8,10)
    
    return []
        


async def download_link(track_url, session,num_song,list_len,semaphore,success_event):
    """
    Downloads the song using the /api/download-track endpoint.
    """
    if not track_url:
        print(f"[{get_timestamp()}] No track URL available for download.")
        return
    
    url = "https://spotymate.com/api/download-track"
    
    # JSON payload for the download request
    data = {
        "url": track_url
    }

    # Headers to mimic a real browser request
    headers = {
        ":authority": "spotymate.com",
        ":method": "POST",
        ":path": "/api/download-track",
        ":scheme": "https",
        "accept": "*/*",
        "accept-encoding": "identity",
        "accept-language": "el-GR,el;q=0.9",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "cookie": "_ga=GA1.1.644048383.1741860429; _ga_7TT8VXDG7S=GS1.1.1741860429.1.1.1741861542.0.0.0",
        "origin": "https://spotymate.com",
        "pragma": "no-cache",
        "referer": "https://spotymate.com/",
        "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    }
    timeout = 40
    
    async with semaphore:  # Limits concurrency
        print(f"[{get_timestamp()}] {num_song}/{list_len} Starting to download {track_url} of the list")
        for i in range(3):  # Retry up to 3 times
            try:
                async with session.post(url, headers=headers, json=data) as response:
                    if response.status == 200:
                        print(f"[{get_timestamp()}] {num_song}/{list_len} Download request successful for {track_url}!")
                        try:
                            response_json = await response.json()
                            downloaded_song = response_json['file_url']
                            #print(downloaded_song)
                            success_event.set()
                            return downloaded_song
                        except Exception as e:
                            print(f"[{get_timestamp()}] {num_song}/{list_len} Error parsing download response: {e}")
                    else:
                        print(f"[{get_timestamp()}] {num_song}/{list_len} Failed to download, Status Code: {response.status}.")
                        success_event.clear()
            except Exception as e:
                print(f"[{get_timestamp()}] {num_song}/{list_len} Error downloading: {e}")
            try:
                # Wait for the success_event to be set with a timeout
                await asyncio.wait_for(success_event.wait(), timeout=timeout)
                print(f"[{get_timestamp()}] {num_song}/{list_len} Retrying..")
            except asyncio.TimeoutError:
                print(f"[{get_timestamp()}] {num_song}/{list_len} Timeout reached, no success event. Proceeding with retry.")
                success_event.clear()  # Reset event if the timeout occurs, and retry
        return None

async def download_file(session, url, songs_folder,semaphore2):
    """ Downloads a file only if it does not already exist. """
    if url is None:
        #print(f"Invalid URL:{url}, skipping...")
        return
    i= 0
    while(i<3):
        i+=1
        try:
            # Extract and decode filename
            filename = os.path.basename(urlparse(url).path)
            filename = unquote(filename)  # Decode URL encoding
            filepath = os.path.join(songs_folder, filename)

            # Check if the file already exists
            if os.path.exists(filepath):
                print(f"[{get_timestamp()}] Skipping (already exists): {filename}")
                return
            async with semaphore2:  # Limits concurrency
                print(f"[{get_timestamp()}] Downloading: {filename}")
                async with session.get(url) as response:
                    if response.status == 200:
                        with open(filepath, "wb") as f:
                            f.write(await response.read())
                        print(f"[{get_timestamp()}] Downloaded: {filename}")
                        return
                    else:
                        print(f"[{get_timestamp()}] Failed to download {filename}, Status: {response.status}")
        except Exception as e:
            print(f"[{get_timestamp()}] Error downloading {url}: {e}")
        finally:
            await asyncio.sleep(5,10)
    return
async def main():
    try:
        if getattr(sys, 'frozen', False):
            os.chdir(os.path.dirname(sys.executable))
        # Read URLs from the CSV file
        base_path = os.getcwd()
        urls_file = os.path.join(base_path, 'urls_file.csv')
        spotify_urls = []
    except Exception as e:
        print(f"Error: {e}")
        await asyncio.sleep(5)
        return 0    
    try:
        with open(urls_file, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                url = row["Spotify urls"]
                spotify_urls.append(url)
    except Exception as e:
        print(f"Error: {e}")
        await asyncio.sleep(5)
        return 0
    
    success_event = asyncio.Event()  # ✅ Create the event
    success_event.set()  # ✅ Initially set to allow first requests
    # Initialize the session and start downloading concurrently
    async with aiohttp.ClientSession() as session:
        download_links = []
        metadata_tasks = []  # List to hold the metadata fetch tasks
        
        # Fetch metadata concurrently for each track URL
        for track_url in spotify_urls:
            metadata_tasks.append(get_metadata(track_url, session))
        
        # Gather all metadata fetch tasks
        all_metadata = await asyncio.gather(*metadata_tasks)
        if(not all_metadata):
            print("Problem with loading the page. Re run the program.")
            await asyncio.sleep(5)
            return 0    
        # Create a new list for download tasks
        download_tasks = []
        download_links = []
        semaphore = asyncio.Semaphore(10)  # Limit concurrency to 15
        all_songs = 0
        for track_urls in all_metadata:
            #print(len(track_urls))
            if track_urls:
                i=0
                list_length = len(track_urls)
                all_songs += list_length
                for track_url in track_urls:
                   i+=1
                   download_tasks.append(download_link(track_url, session,i,list_length,semaphore,success_event))

        # Gather all download tasks
        download_links = await asyncio.gather(*download_tasks)

        # Gather all download tasks
        #download_links = await asyncio.gather(*download_tasks)
        i = 0
        for ik in download_links:
            if(ik is None):
                i+=1
        print(f"Final downloaded links: {len(download_links)- i}/{all_songs}")
        
        songs_folder = os.path.join(base_path, 'songs_folder')  # Use 'songs' instead of 'songs_folder'
        # ✅ Properly check if the folder exists and create it if necessary
        if not os.path.exists(songs_folder):
            print("Creating 'songs' folder...")
            os.makedirs(songs_folder, exist_ok=True)
        else:
            print("'songs_folder' folder already exists.")
        tasks = []
        semaphore2 = asyncio.Semaphore(15) 
        for ik in download_links:
            tasks.append(download_file(session,ik,songs_folder,semaphore2))
        await asyncio.gather(*tasks)
# Run the main async function
asyncio.run(main())
