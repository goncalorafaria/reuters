# reuters

## processing xml and creating bucket chunks.
COLLECTION_DIR = ../proj/

NUM_WORKERS = 4

python3 process_xml.py $NUM_WORKERS $COLLECTION_DIR

## indexing the bucket chunks w/ whoosh.
python3 build.py $NUM_WORKERS

