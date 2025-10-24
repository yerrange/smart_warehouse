def method_1():
    print('method_1')

def method_2():
    print('method_2')

def method_3():
    print('method_3')


METHODS = {'method_1': method_1, 'method_2': method_2, 'method_3': method_3,}


while True:
    command = input()
    if command in METHODS:
        METHODS.get(command)()
    elif command == 'stop':
        break
    else:
        print('unknown command')