BASE_URL="https://freeipapi.com/api/json"
input_file = "data/dsip_excel_file.xlsx"
bucket="ipbk1"
output_file_name= "data/output_new{}.json"
output_object_name="output/output_new{}.json"
max_workers = 50
s3_input_object = "data/data/Sheet_new_{}.csv"
input_sheet_name = 'Sheet_new_{}.csv"'