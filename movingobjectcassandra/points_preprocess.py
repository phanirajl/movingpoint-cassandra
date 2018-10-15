# KETENTUAN
# id di kolom 1
# latitude di kolom 2
# longitude di kolom 3


import csv
import sys

f = open(sys.argv[1])
csv_f = csv.reader(f, delimiter=';')
headers = next(csv_f, None)
first = next(csv_f, None)
column_number = len(headers)
print headers
print first[3]
print column_number
print sys.argv[1]
i_id = 0
i_lat = 1
i_long = 2
current_id = first[i_id]
component_list = []
absis_list = []
ordinat_list = []
absis_list.append(float(first[i_long]))
ordinat_list.append(float(first[i_lat]))
component_list.append("{absis: " + first[i_long] + ", ordinat: " + first[i_lat] + "}")
descriptive_attrs = []
i=0
while i < len(first):
	if i==i_lat:
		i+=1
	if i==i_long:
		i+=1
	descriptive_attrs.append(first[i])
	i+=1
results = []
no_component = 1

with open('new.csv', "wb") as csv_file:
	writer = csv.writer(csv_file, delimiter=';')
	for row in csv_f:
		if row[i_id] == current_id:
			component_list.append("{absis: " + row[i_long] + ", ordinat: " + row[i_lat] + "}")
			no_component = no_component + 1
			absis_list.append(float(row[i_long]))
			ordinat_list.append(float(row[i_lat]))
		else:
			print row[i_id]
			i=0
			while i < len(descriptive_attrs):
				results.append(descriptive_attrs[i])
				i+=1
			results.append("{bounding_box: ({absis: " + str(min(absis_list)) + ", ordinat: " + str(min(ordinat_list)) + "}, {absis: " + str(max(absis_list)) + ", ordinat: " + str(max(ordinat_list)) + "}), no_points: " + str(no_component) + ", point_set: {" + ", ".join(component_list) + "}}")
			writer.writerow(results)
			results = []
			absis_list = []
			ordinat_list = []
			component_list = []
			no_component = 0
			absis_list.append(float(row[i_long]))
			ordinat_list.append(float(row[i_lat]))
			current_id = row[i_id]
			component_list.append("{absis: " + row[i_long] + ", ordinat: " + row[i_lat] + "}")
			no_component = no_component + 1
			descriptive_attrs = []
			i=0
			while i < len(first):
				if i==i_lat:
					i+=1
				if i==i_long:
					i+=1
				descriptive_attrs.append(row[i])
				i+=1

	i=0
	while i < len(descriptive_attrs):
		results.append(descriptive_attrs[i])
		i+=1
	results.append("{bounding_box: ({absis: " + str(min(absis_list)) + ", ordinat: " + str(min(ordinat_list)) + "}, {absis: " + str(max(absis_list)) + ", ordinat: " + str(max(ordinat_list)) + "}), no_points: " + str(no_component) + ", point_set: {" + ", ".join(component_list) + "}}")