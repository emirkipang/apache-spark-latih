def square(arg1=10, arg2=20):
    return (arg1+arg2)

def myfunc(arg1=0):
    if arg1 > 10:
        return 1
    else:
        return 0

def sambutan():
    jam = eval(input("Isikan jam : "))
    if jam >= 6 and jam <=12:
        print("Selamat pagi!")
    elif jam > 12 and jam <= 18:
        print("Selamat sore!")
    elif jam > 18 and jam <= 21:
        print("Selamat malam!")
    else:
        print("Selamat tidur!")
    return jam

print(square())
print(square(10,300))
print(myfunc(200))
sambutan()