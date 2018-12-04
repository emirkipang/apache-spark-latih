tambah = lambda x=1000,y=2000 : x + y

print(tambah())
print(tambah(10,20))

print(type(tambah))


komparasi = lambda a,b : a if (a > 10) else b

print(komparasi(30,10))

size = lambda x : "Standar" if (x == 100) else "Mahal" if (x > 1000) else "Murah"
print(size(1001))

kata = "p-h-y-t-o-n"
kata.replace("-","")
print(kata.replace("-",""))
print(kata[::2]) #stride
kataku = "Harga sepatu nike asli minimal $100"
print(kataku[-4:])
print(kataku.split(" ")[5])

#merk = input("Isi merek sepatu: ")

merk = "Adidas"
cust = "Andika"
kalimat = "Sepatu dengan merk {} baru saja dibeli {}"

print(kalimat.format(merk,cust))
#print("Sepatu dengan merk "+merk+" baru saja dibeli "+cust)
