from prettytable import PrettyTable

def intInput(text: str, sign: str = "=", limit: int = 0):
    while True:
        c = input(text + "\n")
        try:
            c = int(c)
            if sign == "=":
                return c
            elif sign == "+" and c > limit:
                return c
            elif sign == "-" and c < limit:
                return c
            print("Ingrese un valor que cumpla con el criterio solicitado.")
        except:
            print("Ingrese un valor valido.")


def positiveIntInput(text: str, limit: int = 0, infLimit: int = 0):
    while True:
        c = input(text + "\n")
        try:
            c = int(c)
            if c > infLimit and c <= limit:
                return c
            print(f'''El valor ingresado debe de estar en el rango de {infLimit + 1} y {limit}.''')
        except:
            print("Ingrese un valor valido.")


def floatInput(text: str, sign: str = "=", limit: float = 0):
    while True:
        c = input(text + "\n")
        try:
            c = float(c)
            if sign == "=":
                return c
            elif sign == "+" and c > limit:
                return c
            elif sign == "-" and c < limit:
                return c
            print("Ingrese un valor que cumpla con el criterio solicitado.")
        except:
            print("Ingrese un valor valido.")


def selectOptionInList(text: str,  options: list, errorString:str = "Ingrese un numero de opcion valido") -> int:
    while True:
        print(text + "\n")
        for index, value in enumerate(options):
            print(str(index + 1) + ") " + str(value))
        c = intInput("Ingrese el numero de la opcion que desea", sign="+")
        if c <= len(options):
            return c - 1
        else:
            print(errorString)


def printModel(model):
    keys = [key for key in model.__properties__.keys()]
    values = [model.__properties__[key] for key in keys]
    x = PrettyTable()
    x.field_names = keys
    x.add_row(values)
    print(x)

def printModels(models: list):
    if len(models) > 0:
        keys = [key for key in models[0].__properties__.keys()]
        x = PrettyTable()
        x.field_names = keys
        for model in models:
            model = model.__properties__
            values = [model[key] for key in keys]
            x.add_row(values)
        print(x)
    else:
        print("No hay modelos")








