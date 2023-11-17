# wikipedia-dump-link-parser

A small tool for pulling the links from every page on Wikipedia, for the purpose of mapping.

# WIP

This is a WIP, currently it does not do anything with the resolved data.

# Usage

Get a dump of wikipedia from [here.](https://en.wikipedia.org/wiki/Wikipedia:Database_download)
You need both the multistream XML dump, and the index file. Make sure they are from the same dump date.

Place the compressed xml.bz2 dump into the root of the project and name it `dump.xml.bz2`
Split the index into however many chunks you would like and put it in `index/chunk_name`

> The chunks can be named whatever you like, a process will be launched per chunk.

Customize the number of threads per process by editing the wikitool.py file.

Run with `python3 wikitool.py`