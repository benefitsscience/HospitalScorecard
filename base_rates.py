# import gzip
#
#
# def gunzip_file(gz_file_path, extract_to_path):
#     with gzip.open(gz_file_path, 'rb') as gz_file:
#         with open(extract_to_path, 'wb') as extract_file:
#             extract_file.write(gz_file.read())
#
#
# gz_file_path = 'table_Matched.gz'
# extract_to_path = 'table_Matched.txt'
# gunzip_file(gz_file_path, extract_to_path)


import pandas as pd

pd.set_option('display.max_columns', None)
base_rates = pd.read_csv('base_rates.txt', sep=',')

base_rates = base_rates[base_rates['code'] == '807]
base_rates.to_csv('base_rates.csv', index=False)
print(base_rates)