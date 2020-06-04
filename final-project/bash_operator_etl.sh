echo 'starting python functions'

echo 'creating lookup json file for labels using SAS label description'
python sas_labels_json.py

echo ' labels created'

echo 'creating mapping tables from the json created and creating dim_date and dim_us_demographics'

python mapping_tables.py


echo 'tables created successfully'

echo ' performing now etl by running etl.py file'

python etl.py

echo 'finished all jobs successfully'

