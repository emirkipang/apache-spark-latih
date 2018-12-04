from os.path import basename

with open("out/airports_pairrdd.txt/.part-00000.crc", 'r') as myfile:
    data = myfile.read()
print str.replace(basename("out/airports_pairrdd.txt/.part-00000.crc"),".crc","")