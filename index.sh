
echo "Processing the xml and creating 5 chunks."
py process_xml.py

echo "Indexing everything."
py build.py 5
