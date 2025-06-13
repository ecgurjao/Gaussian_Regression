# coding: utf-8
# @autor Thyago Pereira da Silva 
import math
import csv
from math import radians, cos, sin, asin, sqrt 
import random
from datetime import datetime, timedelta, date

# -- O codigo faz muita consulta ao disco, perdemos cerca de  10 ^ -3 segundos a cada consulta --#
# -- Buscaremos usar threads ou auxilio do sistema operacional para paralelismo de processos -- #


# -- Retorna a distância(KM) entre dois pontos da terra (levando em conta a curvatura) -- # 
def get_distance(cord1,cord2):
    lat1 = radians(cord1["lat"])
    lng1 = radians(cord1["lng"])

    lat2 = radians(cord2['lat'])
    lng2 = radians(cord2['lng'])

    dlat = lat2 - lat1
    dlng = lng2 - lng1  

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlng / 2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371

    return(c * r) 

# -- Acessa a base de ceps e retorna as cordenadas associadas a ele formato LAT,LNG -- #
def get_cord_cep(cep):
    with open('./Data/Cordenadas_Ceps.csv') as file : 
        reader  =  csv.DictReader(file)
        cord  = {'lat': 0 , 'lng': 0}
        for row in reader :
            if (row['Cep'] == cep):
                cord['lat'] = float(row['Lat'])
                cord['lng'] = float(row['Lng'])
                return cord 
        
        raise UnboundLocalError('Não foi possivel associar Lat e Lng a esse cep')

#-- Retorna uma lista de ruas dentro do raio(km) de infecção --#
def affected_area(cep,raio):
    afetadas = []
    cord1 = get_cord_cep(cep)
    with open('./Data/Cordenadas_Ceps.csv') as file:
        reader  = csv.DictReader(file)
        for row in reader:
            cord2 = get_cord_cep(row['Cep'])
            distance =  get_distance(cord1,cord2)
            if(distance <= raio):
                afetadas.append(row['Cep'])

    return afetadas

#-- Retorna o numero de casos dentro do Raio de infecção gerado --#
def number_ofCases_radius(afected_streets,base):
    with open(base +'.csv') as file:
        reader =  csv.DictReader(file)
        ceps = []
        for row in reader: 
            ceps.append(row["Cep"])

        count = 0
        for street in afected_streets:
            count += ceps.count(street)
        
        return count

#  -- Gera uma Lista com todos os dias de infecção do covid até o atual  --  #
def generate_date_interval():
    sdate = date(2020, 3, 27)   # start date - Primeiro Caso confirmado em CG - PB
    edate = date.today()        # end date - Dia de Hj

    delta = edate - sdate       # as timedelta
    days = []
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        days.append(day)
    
    return days

# --  Retorna uma base de dados ficticia dentro do Raio de infecção gerado --#
# --  Para simplificar execute como python Affected.py > randomDatabase.csv --#
def randomDataBase(cep,raio_km,size):
    afected_streets =  affected_area(cep,raio_km)
    datas = generate_date_interval()
    datas_sort = []
    for i in range(size):
        datas_sort.append(datas[i].strftime("%d/%m/%Y"))
 
    datas_sort.sort()
    for i in range(size):        
        local =  random.choice(afected_streets)
        print(local + "," + datas_sort[i])

#Gera uma base de dados aleátoria pronta para o processo gauseano  
def randomDataBaseGausean(cep,raio_km,size):
    afected_streets =  affected_area(cep,raio_km)
    datas = generate_date_interval()
    datas_sort = []
    cases_fictional = []
    
    for i in range(size):
        datas_sort.append(datas[i].strftime("%d/%m/%Y"))
        cases_fictional.append(random.randint(1,2000))
        
    print("Data,casos")
    datas_sort = sorted(datas_sort)
    cases_ficional =  sorted(cases_fictional)
    
    for i in range(size):        
        local =  random.choice(afected_streets)
        print(datas_sort[i]+ "," + str(cases_ficional[i]))

def get_data(dias, day):
    return dias.count(day)

def get_acumulada(casos):
    acumulada = []
    for caso in casos:
        acumulada.append(caso['casos'])

    for i in range(1,len(acumulada),1):
        acumulada[i] = acumulada[i] + acumulada[i -1]
    
    i = 0
    acumulative = []
    for caso in casos:
        case = {
            'data': caso['data'],
            'cases': acumulada[i]
        }
        i += 1
        acumulative.append(case)

    return acumulative

#Filtra a base de casos Reais com o numero de casos que ocorreram em determinado cep
    # Mantendo  consistencia do numero de dias oferecidos pela base real 

def real_database(cep,raio):
    afected_streets =  affected_area(cep,raio) 
    casos = []       
    with open("./Data/BaseFiltrada.csv") as file:
        reader  = csv.DictReader(file) 
        aux = ''
        for row in reader:
            day  =  row['Data']
            aux =  day.split('/')
            if(len(aux) == 3):
                day  = date(int(aux[2]),int(aux[1]),int(aux[0]))
                caso = {
                    'dia' : day,
                    'cep' : row["Cep"]
                }

                casos.append(caso)
    
    interested_case = []
    for caso in casos:
        if(caso['cep'] in afected_streets):
            interested_case.append(caso)

    interested_case = sorted(interested_case, key = lambda row:row['dia'])
    todas_datas = generate_date_interval()
    cases_datas = []

    for caso in interested_case:
        cases_datas.append(caso['dia'])
    
    datas_zeradas = [] 

    for data in todas_datas:
        if(data not in cases_datas):
            datas_zeradas.append(data)
    
    final =[]
    for day in todas_datas:
        ontem = day - timedelta(days=1)
        if((day in datas_zeradas) and (ontem not in todas_datas)):
                caso = {
                    "data" : day,
                    'casos': 0
                }
        elif((day in datas_zeradas) and (ontem in cases_datas)):
                caso = {
                    'data': day,
                    'casos': get_data(cases_datas,ontem) 
                }
        else:
            caso = {
                'data': day,
                'casos': get_data(cases_datas,day)
            }
        
        final.append(caso)

    acumulada  =  get_acumulada(final)     

    print('dia,cases')
    for caso in acumulada:

        print(caso['data'].strftime("%d/%m/%Y") +',' + str(caso['cases']) )

real_database('58407325',1)