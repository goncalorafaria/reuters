# reuters

## processing xml and creating bucket chunks.
COLLECTION_DIR = ../proj/

python3 process_xml.py 5 $COLLECTION_DIR

## indexing the bucket chunks w/ whoosh.
python build.py 5

