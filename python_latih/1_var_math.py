# a,b,c = 100,200,350
# print(a)
# num = (a+b+c)/30
# print(num)
# print((a+b+c)//30)
# print((a+b+c)%30)
#
# x=[] #list
# y=() #tuple/array
# z={} #dictionary
#
# angka = list(range(10,150,10))
# print(angka)
#
# keranjang = ["apel","jeruk","nanas"]
# keranjang2 = [10,20,30]
# keranjang3 = keranjang + keranjang2
# keranjang4 = [keranjang, keranjang2]
# print(keranjang3)
# print(keranjang4)
# print(keranjang3.count("nanas"))
# print(keranjang4[0].count("nanas"))
# print(keranjang4[1][0]+keranjang4[1][2])
#
# tupangka1 = (40,50,40,'70')
# tupangka2 = tuple(keranjang2)
# print(tupangka2)
# tupangka3 = (tupangka1, tupangka2)
# print(tupangka3)
# print((tupangka3[0][0]+tupangka3[1][2])/tupangka3[1][1])
# tupangka4 = tupangka1 + tupangka1
# print(tupangka4)

# d1 = {}
# d1 ["A"] = 10
# d1 ["B"] = 20
# d1 ["C"] = 50
# print(d1)
# mainan = {"robot" : 1500, "car" : 2000}
# print(mainan)
# mainan.update(d1)
# print(mainan)
# print(list(mainan.values()))
# print(list(mainan.keys()))
# maze = {"K1":list(range(4)), "K2":tuple(range(4,8)), "K3":(1,2,3,{"K4":[1,2,3,"Found you!",4,5]})}
# print(maze["K3"][3]["K4"][3])

#1
toys = {"robot": "$40.0", "car": "$25", "ironman": "$12"}
print(int(eval(list(toys.values())[0][1:])+eval(list(toys.values())[1][1:])+eval(list(toys.values())[2][1:])))

#2
questions = [10 != 4, 50 == 50, 90 == 10, "c" in ("a", "b", "c"), 100 != 100]
print(questions)

#3
films = {"k1": "blade runner 2049", "k2": "matrix", "k3": "terminator"}
print(len(films["k1"]) > len(films["k2"]) < len(films["k3"]))

#4
life_stages = {0: "embryo", 1: "fetus", 2:"baby", 3:"infant",4: "teen"}

midlife = {}
midlife[5] =  "adults"
midlife[6] =  "big kid!"
life_stages.update(midlife)
print(life_stages)

#5
nest1 = [(1,2,3), {"k1": [8, 1, 300, 2, 77], "k2": [10,20,30]}, ["a","500", "c"]]
print(float(sorted(nest1[0])[-1] + sorted(nest1[1]["k1"])[-1] + sorted(nest1[1]["k2"])[-1] + eval(sorted(nest1[2])[0])))

#6
prices = ["a", "b", "9", "c", "d", "FOUR", "e", "f", "2.5"]
cash = prices[2::3]
sentence = """The bill for the {}#!/,?? {}#!/ ??and drink came to {}??"""
# 'The bill for the pizza, chips and drink came to $15.5'
print(cash)
print(sentence.replace("{}#!/,?? {}#!/ ??", "pizza, chips ").replace("{}??", "$15.5"))



