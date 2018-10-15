# KETENTUAN
# id di kolom 1
# latitude di kolom 2
# longitude di kolom 3
# timestamp di kolom 4

import csv
import sys
from datetime import datetime

f = open(sys.argv[1])
csv_f = csv.reader(f, delimiter=';')
headers = next(csv_f, None)
first = next(csv_f, None)
i_id = 0
i_lat = 1
i_long = 2
i_timestamp = 3
current_id = first[3]
current_tuple = (float(first[i_long]), float(first[i_lat]), first[i_timestamp])
point_list = []
absis_list = []
ordinat_list = []
time_list = []
absis_list.append(float(first[i_long]))
ordinat_list.append(float(first[i_lat]))
time_list.append(datetime.strptime(first[i_timestamp], '%Y-%m-%d %H:%M:%S'))

point_list.append(current_tuple)
component_list = []
component_list.append("{p: {absis: " + first[i_long] + ", ordinat: " + first[i_lat] + "}, t: '" + first[i_timestamp] + "'}")
descriptive_attrs = []
i=0
while i < len(first):
	if i!=i_lat and i!=i_long and i!=i_timestamp:
		descriptive_attrs.append(first[i])
	i+=1
results = []

no_component = 1

with open('new.csv', "wb") as csv_file:
	writer = csv.writer(csv_file, delimiter=';')
       

	for row in csv_f:
		if row[i_id] == current_id:
			point_list.append((float(row[i_long]), float(row[i_lat]), row[i_timestamp]))
			absis_list.append(float(row[i_long]))
			ordinat_list.append(float(row[i_lat]))
			time_list.append(datetime.strptime(row[i_timestamp], '%Y-%m-%d %H:%M:%S'))
			component_list.append("{p: {absis: " + row[i_long] + ", ordinat: " + row[i_lat] + "}, t: '" + row[i_timestamp] + "'}")
			no_component = no_component + 1
		else:
			i=0
			while i < len(descriptive_attrs):
				results.append(descriptive_attrs[i])
				i+=1
			# results.append(point_list)
			# results.append(absis_list)
			# results.append(ordinat_list)
			# results.append(min(absis_list))
			# results.append(min(ordinat_list))
			# results.append(max(absis_list))
			# results.append(max(ordinat_list))
			# results.append(min(time_list).strftime('%Y-%m-%d %H:%M:%S'))
			results.append("{bounding_box: ({absis: " + str(min(absis_list)) + ", ordinat: " + str(min(ordinat_list)) + "}, {absis: " + str(max(absis_list)) + ", ordinat: " + str(max(ordinat_list)) + "}), no_components: " + str(no_component) + ", lifespan: ('" + min(time_list).strftime('%Y-%m-%d %H:%M:%S') + "', '" + max(time_list).strftime('%Y-%m-%d %H:%M:%S') + "'), component_set: {" + ", ".join(component_list) + "}}")
			# print results[0]
			writer.writerow(results)
			results = []
			time_list = []
			point_list = []
			absis_list = []
			ordinat_list = []
			component_list = []
			no_component = 0
			absis_list.append(float(row[i_long]))
			ordinat_list.append(float(row[i_lat]))
			time_list.append(datetime.strptime(row[i_timestamp], '%Y-%m-%d %H:%M:%S'))
			current_id = row[3]
			point_list.append((float(row[i_long]), float(row[i_lat]), row[i_timestamp]))
			component_list.append("{p: {absis: " + row[i_long] + ", ordinat: " + row[i_lat] + "}, t: '" + row[i_timestamp] + "'}")
			no_component = no_component + 1
			component_list.append("{p: {absis: " + first[i_long] + ", ordinat: " + first[i_lat] + "}, t: '" + first[i_timestamp] + "'}")
			descriptive_attrs = []
			i=0
			while i < len(first):
				if i!=i_lat and i!=i_long and i!=i_timestamp:
					descriptive_attrs.append(row[i])
				i+=1

	i=0
	while i < len(descriptive_attrs):
		results.append(descriptive_attrs[i])
		i+=1
			# results.append(point_list)
			# results.append(absis_list)
			# results.append(ordinat_list)
			# results.append(min(absis_list))
			# results.append(min(ordinat_list))
			# results.append(max(absis_list))
			# results.append(max(ordinat_list))
			# results.append(min(time_list).strftime('%Y-%m-%d %H:%M:%S'))
	results.append("{bounding_box: ({absis: " + str(min(absis_list)) + ", ordinat: " + str(min(ordinat_list)) + "}, {absis: " + str(max(absis_list)) + ", ordinat: " + str(max(ordinat_list)) + "}), no_components: " + str(no_component) + ", lifespan: ('" + min(time_list).strftime('%Y-%m-%d %H:%M:%S') + "', '" + max(time_list).strftime('%Y-%m-%d %H:%M:%S') + "'), component_set: {" + ", ".join(component_list) + "}}")
	# print results[0]
	writer.writerow(results)



# {bounding_box: ({absis: 4.91664, ordinat: 48.12994}, {absis: 14.46598, ordinat: 52.34997}), no_components: 3, lifespan: ('2014-01-09 13:45:25', '2014-03-09 16:33:31'), component_set: {{p: {absis: 4.91664, ordinat: 52.34997}, t: '2014-02-09 16:33:31'}, {p: {absis: 11.57499, ordinat: 48.12994}, t: '2014-01-09 13:45:25'}, {p: {absis: 14.46598, ordinat: 50.08334}, t: '2014-03-09 16:33:31'}}}
# {p: {absis: 4.91664, ordinat: 52.34997}, t: '2014-02-09 16:33:31'}
# {p: {absis: -37.0627421097422, ordinat: -10.9393413858164}, t: 2014-09-13 07:24:32}


