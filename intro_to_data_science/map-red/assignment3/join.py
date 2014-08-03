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
    table = record[0]
    key = record[1]
    val = table + '>'

    temp = ''    
    for f in record:
        temp = temp + f + "<" 

    temp = temp[:-1]



    val = val + temp

    mr.emit_intermediate(key,val )

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts


    for t in list_of_values:
	t1 = t.split(">")[0]
        for r in list_of_values:
	    total_fields = []
	    t2 = r.split(">")[0]
	    if t1 != t2:
		for v in t.split(">")[1].split("<"):
		    total_fields.append(v)
		for v in r.split(">")[1].split("<"):         
                    total_fields.append(v)

	        mr.emit(total_fields)
	list_of_values.remove(t)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
