# reuters

Creates an inverted index for the reuters collection.






COLLECTION_DIR = ../proj/

NUM_WORKERS = 4

// processing xml and creating bucket chunks.

python3 create.py $NUM_WORKERS $COLLECTION_DIR

// indexing the bucket chunks w/ whoosh.

python3 build.py $NUM_WORKERS

This will create 4 files called chunk0, chunk1, chunk2, chunk3 and a directory called  indexdir w/ a whoosh index.

To load everything into memory just do Bucket(). We can specify also from where to load or what to load but if the script is running on the repository directory no arguments works well. 


main.py has the API for the functionalities of the project assignment. 

create.py build.py does the processing and index creation.
core.py defines the main objects of interest and functionality.
utils.py implements usefull things that used in the project but that dont have dependencies inside the project.
vizualization.py creates the plots for the report.

