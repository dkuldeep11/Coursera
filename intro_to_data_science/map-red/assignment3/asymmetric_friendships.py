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
    key = record[0] + ":" + record[1]
    mr.emit_intermediate(key, 1)
    key = record[1] + ":" + record[0]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    if len(list_of_values) == 1:
        #pair = "(" + key.split(":")[0] + "," + key.split(":")[1] + ")"	
        pair = tuple(key.split(":"))
	#print pair
        #mr.emit(key.split(":"))
        mr.emit(pair)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
