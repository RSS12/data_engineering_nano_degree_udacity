import os
import glob
import pandas as pd
import csv

for  root, dirs, files in os.walk('./dm_nosql/event_data'):
    # print(root)
    # print(dirs)
    # print(files)
    file_path_list = glob.glob(os.path.join(root, '*'))
    print(file_path_list)


df = pd.DataFrame()

for f in file_path_list:
    data = pd.read_csv(f,sep=',')
    df = df.append(data)



sample_df = df[['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId']].fillna('')


sample_df = sample_df[sample_df['artist']!='']


sample_df_list = sample_df.values.tolist()

for row in sample_df_list:
    print("Artist: {}\n"
          "FirstName: {}".
          format(row[0],row[1]))




# #
# df_list = sample_df.values.tolist()
#
# df_list_sample = df_list[:15]
#
# for row in  df_list_sample:
#     if row[0]=='':
#         continue
#     print(row[0], row[1],row[2],row[3])


# initiating an empty list of rows that will be generated from each file
# full_data_rows_list = []
#
# # for every filepath in the file path list
# for f in file_path_list:
#
#     # reading csv file
#     with open(f, 'r', encoding='utf8', newline='') as csvfile:
#         # creating a csv reader object
#         csvreader = csv.reader(csvfile)
#         next(csvreader)
#
#         # extracting each data row one by one and append it
#         for line in csvreader:
#             # print(line)
#             full_data_rows_list.append(line)
#
#         # uncomment the code below if you would like to get total number of rows
# print(len(full_data_rows_list))
# # uncomment the code below if you would like to check to see what the list of event data rows will look like
# print(full_data_rows_list)
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = []

# for every filepath in the file path list
for f in file_path_list:

    # reading csv file
    with open(f, 'r', encoding='utf8', newline='') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        next(csvreader)

        # extracting each data row one by one and append it
        for line in csvreader:
            # print(line)
            full_data_rows_list.append(line)

csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
