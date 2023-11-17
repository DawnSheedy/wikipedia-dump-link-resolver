import xml.etree.ElementTree as ET
import bz2
import time
import concurrent.futures
import os
from multiprocessing import Process
from neo4j import GraphDatabase

dump_file = "dump.xml.bz2"
max_count = -1
max_thread = 8

# threadpool
pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_thread)

file_chunks = {}

# load file
infile = open(dump_file, "rb")

def create_node_tx(tx, name, sub_pages, is_redirect):
    query = ("MERGE (p:Page { slug: $name }) "
                 "ON CREATE SET p.isRedirect = $is_redirect "
                 "ON MATCH SET p.isRedirect = $is_redirect")
    result = tx.run(query, name=name, is_redirect=is_redirect)
    for sub_page in sub_pages:
        sub_query = ("MERGE (outPage:Page { slug: $sub_page }) "
                    "WITH outPage "
                    "MATCH (page:Page { slug: $name }) "
                    "MERGE (page)-[rel:LINKS_TO]->(outPage)")
        tx.run(sub_query, name=name, sub_page=sub_page)
    record = result.single()

db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "testpassword",), max_connection_pool_size=64, max_transaction_retry_time=9999999)

# queue a chunk of the index
def queue_index_chunk(file_name):
    db.verify_connectivity()
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
def process_page(uncompressed_data, page_id, title):
    page_text = get_wikitext(uncompressed_data, page_id=int(page_id))
    if page_text is None:
        raise Exception('Unable to resolve page text for: ' + title)
    is_redirect = page_text.startswith("#REDIRECT")
    page_links = get_linked_articles(page_text)
    global db
    with db.session(database='wikilinks') as session:
        try:
            node_id = session.execute_write(create_node_tx, title, page_links, is_redirect)
        except Exception as e:
            print(e)
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
