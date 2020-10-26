# reuters

## processing xml and creating bucket chunks.
COLLECTION_DIR = ../proj/

NUM_WORKERS = 4

python3 process_xml.py 5 $COLLECTION_DIR

## indexing the bucket chunks w/ whoosh.
python3 build.py 5 $NUM_WORKERS

