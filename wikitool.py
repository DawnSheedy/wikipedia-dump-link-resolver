import xml.etree.ElementTree as ET
import bz2
import time
import concurrent.futures
import os
from multiprocessing import Process

dump_file = "dump.xml.bz2"
max_count = -1
max_thread = 64

# threadpool
pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_thread)

file_chunks = {}

# load file
infile = open(dump_file, "rb")

# queue a chunk of the index
def queue_index_chunk(file_name):
    print('Reading index chunk '+file_name)
    with open(file_name, 'r') as file:
        Lines = file.readlines()
        lines_processed = 0
        start_time = time.time()
        last_data = b""
        last_chunk = ''

        for line in Lines:
            line = line.strip()
            first_colon = line.find(":")
            second_colon = line.find(":", first_colon+1)
            offset = line[:first_colon]
            page_id = line[first_colon+1:second_colon]
            title = line[second_colon+1:]
            if (offset != last_chunk):
                last_data = load_file_chunk(offset)
                last_chunk = offset
            pool.submit(process_page, last_data, page_id, title)
            lines_processed+=1
            if (max_count > -1 and lines_processed >= max_count):
                break

    print('All Jobs Queued for Chunk.', file_name)
    pool.shutdown(wait=True)
    end_time=time.time()
    infile.close()
    print('All Jobs Done for Chunk.', file_name, (end_time-start_time)*1000.0)

# load a chunk of the multistream file
def load_file_chunk(offset, block_size=256*1024):
    unzipper = bz2.BZ2Decompressor()

    # Read the compressed stream, decompress the data                                                                                                                                                        
    uncompressed_data = b""
    infile.seek(int(offset))

    while True:
        compressed_data = infile.read(block_size)
        try:
            uncompressed_data += unzipper.decompress(compressed_data)
        except EOFError:
            # We've reached the end of the stream                                                                                                                                                        
            break
        # If there's no more data in the file                                                                                                                                                            
        if compressed_data == '':
            # End if we've finished reading the stream                                                                                                                                                   
            if unzipper.need_input:
                break
                # Otherwise we've failed to correctly read all of the stream                                                                                                                                 
            raise Exception("Failed to read a complete stream")
    return uncompressed_data

# Get the raw text of a page_id within a chunk
def get_wikitext(uncompressed_data, page_id=None, title=None, namespace_id=None, verbose=True):                                                                                                                                                                                        
    uncompressed_text = uncompressed_data.decode("utf-8")
    xml_data = "<root>" + uncompressed_text + "</root>"
    root = ET.fromstring(xml_data)
    for page in root.findall("page"):
        if title is not None:
            if title != page.find("title").text:
                continue
        if namespace_id is not None:
            if namespace_id != int(page.find("ns").text):
                continue
        if page_id is not None:
            if page_id != int(page.find("id").text):
                continue
        # We've found what we're looking for                                                                                                                                                                 
        revision = page.find("revision")
        wikitext = revision.find("text")
        return wikitext.text

    # We failed to find what we were looking for                                                                                                                                                             
    return None

# Find all the links within an article
def get_linked_articles(text):
    cur_index = 0
    links = []
    while True:
        # find start of link
        cur_index = text.find("[[", cur_index)
        # No more links, break
        if cur_index == -1:
            break
        end_of_link = text.find("]]", cur_index)
        link_body = text[cur_index+2:end_of_link]

        link_separator = link_body.find('|')

        if (link_separator != -1):
            link_body = link_body[:link_separator]

        links.append(link_body)
        cur_index+=2
    return links

# Process a given page
def process_page(offset, page_id, title):
    page_text = get_wikitext(int(offset), page_id=int(page_id))
    if page_text is None:
        raise Exception('Unable to resolve page text for: ' + title)
    is_redirect = page_text.startswith("#REDIRECT")
    page_links = get_linked_articles(page_text)
    # TODO, do something with this data
    
# Print thread info
def info(title):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())

# If we're on the main thread, start other threads
if __name__ == '__main__':
    info('Main Thread')
    for file in os.listdir('index'):
        if (file == '.DS_Store'):
            continue
        process = Process(target=queue_index_chunk, args=('index/'+file,))
        process.start()
