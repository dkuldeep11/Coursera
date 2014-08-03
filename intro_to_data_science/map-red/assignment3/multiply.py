import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    #print record
    name = record[0]
    i = record[1]
    j = record[2]
    val = record[3]

    if name == "a":
	for k in range(0,5):
	    mr.emit_intermediate((i,k),(j,val))      

    if name == "b":
	for k in range(0,5):
            mr.emit_intermediate((k,j),(i,val))

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #print key, list_of_values
    sum = 0
    for i in range(0,len(list_of_values)):
        v1 = list_of_values[i]
	for j in range(i+1, len(list_of_values)):
	    v2 = list_of_values[j]
	    if v1[0] == v2[0]:
		#print v1, v2
		sum = sum + v1[1] * v2[1]
		#list_of_values.remove(v1)
		#list_of_values.remove(v2)
		break
    l1 = list(key)
    l1.append(sum)
    mr.emit(tuple(l1))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
