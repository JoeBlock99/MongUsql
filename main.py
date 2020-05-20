from pymongo import MongoClient
import time
from datetime import datetime, timedelta
import psycopg2
from utils import intInput, positiveIntInput
from prettytable import PrettyTable

x = PrettyTable()
# Pymongo
client = MongoClient('localhost', 27017)
db = client.Project2

# Psycopg2
conn = psycopg2.connect("dbname=streaming user=postgres password=Diciembre98")
cur = conn.cursor()



# Functions
def pull_data_origin(date):
    invcol = db["invoices"]
    cur.execute(
        "SELECT FirstName, LastName, email, InvoiceDate, InvoiceId, Total FROM Invoice JOIN Customer on Invoice.CustomerId = Customer.CustomerId WHERE InvoiceDate = '{}' ".format(
            date))
    a = cur.fetchall()
    x.field_names = ["Primer nombre", "Apellido", "email", "Fecha de compra", "total"]
    for i in a:
        data = {
            'FirstName': i[0],
            'LastName': i[1],
            'email': i[2],
            'InvoiceDate': i[3],
            'InvoiceId': i[4],
        }
        db.customers.update({'Total': str(i[5])},{
            'FirstName': i[0],
            'LastName': i[1],
            'email': i[2],
            'InvoiceDate': i[3],
            'InvoiceId': i[4],
        }, upsert=True)
        x.add_row([i[0], i[1], i[2], i[3], i[5]])
    print(x)
    print("\n" * 2)


# 2009-02-01
def return_ten_custumers_and_newtracks():
    cur.execute("SELECT customerid, firstname, lastname FROM customer ORDER BY RANDOM() LIMIT 10")
    a = cur.fetchall()
    for client in a:
        print("-"*4 + f"Canciones recomendadas para {client[1]} {client[2]}" + "-"*4 + "\n\n")
        cur.execute(f'''
        select track.name, track.trackid, track.unitprice from track
        inner join invoiceline on invoiceline.trackid = track.trackid
        inner join invoice on invoiceline.invoiceid = invoice.invoiceid
        where invoice.CustomerId != {client[0]}
        group by (track.trackid)
        order by track.trackid desc 
        limit 10
        ''')
        y = PrettyTable()
        y.field_names = ["Nombre de canción", "precio"]
        b = cur.fetchall()
        for track in b:
            y.add_row([track[0], track[1]])
        print(y)
        print("\n"*2)



selectedYear = positiveIntInput("Ingrese el año que desea guardar las ventas en mongo.\nDebe de estar en el rango de 2009 y 2021\n", 2021, 2008)
selectedMonth = positiveIntInput("Ingrese el numero de mes del año", 12)
maxDay = 31
if selectedMonth == 2:
    maxDay = 28
selectedDay = positiveIntInput(f"Ingrese el numero de dia del año {selectedYear} y mes {selectedMonth}", maxDay)
pull_data_origin(f'{selectedMonth}/{selectedDay}/{selectedYear}')

return_ten_custumers_and_newtracks()