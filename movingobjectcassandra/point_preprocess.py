import csv
import sys

f = open(sys.argv[1])
csv_f = csv.reader(f)
headers = next(csv_f, None)
column_number = len(headers)
print headers
print column_number
print sys.argv[1]
i_lat = headers.index("lat")
i_long = headers.index("long")
results = []

for row in csv_f:
	new_row = []
	i=0
	while i < len(row):
		if i==i_lat:
			row[i_lat] = "{absis:" + row[i_long] + ", ordinat:" + row[i_lat] + "}"
		elif i==i_long:
			i+=1	
		new_row.append(row[i])
		i+=1
	results.append(new_row)

print "done"

with open('new.csv', "wb") as csv_file:
    writer = csv.writer(csv_file, delimiter=',')
    for r in results:
        writer.writerow(r)