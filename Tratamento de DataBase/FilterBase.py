#coding: utf - 8 
#@autor Thyago Pereira da Silva
import csv

# -- Verifica se o Cep Fornecido consta na base de dados oficial --#
def valida_cep(cep):
    valido  = False
    with open('./Data/Cordenadas_Ceps.csv') as file:
        reader  = csv.DictReader(file)
        if(cep =='00000000'):
            return valido
        for row in reader:
           if(row["Cep"] == cep):
                valido = True
    
    return valido


# -- Coloca o cep no Formato aceito para comparações -- # 
def formata_cep(cep):
    formatado  = ''
    for simbol in cep :
        if( (simbol != ".") and (simbol != "-")):
            formatado += simbol

    return formatado


# -- Codigo dedicado á filtragem de uma base de dados para lidar apenas com o pertinente -- #
# -- Para gerar o csv resultante excutar como FilterBase.py > BaseFiltrada.csv -- #
def main():
    with open('./Data/Base.csv')as file:
        reader  = csv.DictReader(file)
        print("Cep,Data")
        for row in reader:
            cep =  formata_cep(row["CEP"])
            if((row['MunicÃ­pio da NotificaÃ§Ã£o'] == "Campina Grande") and (row["Resultado do Teste"] == "Positivo")):
                if(valida_cep(cep)):
                    print(cep  + "," + row["Data da NotificaÃ§Ã£o"])  

# -- Codigo Sujeito á Alteração (de modo que a base varia ) -- #

main()



