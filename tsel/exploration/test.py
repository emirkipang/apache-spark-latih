x=""

def func():
    global x
    x = x + "|abc"

def func2():
    global x
    x = x + "|abc"
    x = x + "|bcd"

def func3():
    global x
    print(x)

func()
func2()
func3()