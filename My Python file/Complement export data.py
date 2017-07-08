import pandas as pd
import os


cwd = os.getcwd()

read_path = cwd + os.sep + "Original_Data" + os.sep + "export_complement"
complement_path = cwd + os.sep + "Filter_Data" + os.sep + "export"

file_to_complement = {
    "2017": ["01 - Enero"]
}

columns_bultos = ["NUMEROIDENT", "FECHAACEPTPACKAGE", "NUMEROBULTO", "TIPOBULTO",
                 "CANTIDADBULTO", "IDENTIFICACIONBULTO"]

columns_transporte = [ "NUMEROIDENT", "FECHAACEPTTRANSPORT", "NSECDOCTRANSP",
                       "NUMERODOCTRANSP", "FECHADOCTRANSP", "NOMBRENAVE",
                       "NUMEROVIAJE"]

columns_file = [ "FECHAACEPT", "NUMEROIDENT", "ADUANA", "TIPOOPERACION",
                 "CODIGORUTEXPORTADORPPAL", "NRO_EXPORTADOR",
                 "PORCENTAJEEXPPPAL", "COMUNAEXPORTADORPPAL", "CODIGORUTEXPSEC",
                 "NRO_EXPORTADOR_SEC", "PORCENTAJEEXPSECUNDARIO",
                 "COMUNAEXPSECUNDARIO", "PUERTOEMB", "GLOSAPUERTOEMB",
                 "REGIONORIGEN", "TIPOCARGA", "VIATRANSPORTE", "PUERTODESEMB",
                 "GLOSAPUERTODESEMB", "PAISDESTINO", "GLOSAPAISDESTINO",
                 "NOMBRECIATRANSP", "PAISCIATRANSP", "RUTCIATRANSP",
                 "DVRUTCIATRANSP", "NOMBREEMISORDOCTRANSP", "RUTEMISOR",
                 "DVRUTEMISOR", "CODIGOTIPOAUTORIZA", "NUMEROINFORMEEXPO",
                 "DVNUMEROINFORMEEXP", "FECHAINFORMEEXP", "MONEDA",
                 "MODALIDADVENTA", "CLAUSULAVENTA", "FORMAPAGO",
                 "VALORCLAUSULAVENTA", "COMISIONESEXTERIOR", "OTROSGASTOS",
                 "VALORLIQUIDORETORNO", "NUMEROREGSUSP", "ADUANAREGSUSP",
                 "PLAZOVIGENCIAREGSUSP", "TOTALITEM", "TOTALBULTOS",
                 "PESOBRUTOTOTAL", "TOTALVALORFOB", "VALORFLETE", "CODIGOFLETE",
                 "VALORSEGURO", "CODIGOSEG", "VALORCIF", "NUMEROPARCIALIDAD",
                 "TOTALPARCIALES", "PARCIAL", "OBSERVACION",
                 "NUMERODOCTOCANCELA", "FECHADOCTOCANCELA", "TIPODOCTOCANCELA",
                 "PESOBRUTOCANCELA", "TOTALBULTOSCANCELA", "NUMEROITEM",
                 "NOMBRE", "ATRIBUTO1", "ATRIBUTO2", "ATRIBUTO3", "ATRIBUTO4",
                 "ATRIBUTO5", "ATRIBUTO6", "CODIGOARANCEL", "UNIDADMEDIDA",
                 "CANTIDADMERCANCIA", "FOBUNITARIO", "FOBUS",
                 "CODIGOOBSERVACION1", "VALOROBSERVACION1", "GLOSAOBSERVACION1",
                 "CODIGOOBSERVACION2", "VALOROBSERVACION2", "GLOSAOBSERVACION2",
                 "CODIGOOBSERVACION3", "VALOROBSERVACION3", "GLOSAOBSERVACION3",
                 "PESOBRUTOITEM"]


['FECHAACEPT', 'NUMEROIDENT', 'ADUANA', 'TIPOOPERACION', 'CODIGORUTEXPORTADORPPAL',
 'NRO_EXPORTADOR', 'PORCENTAJEEXPPPAL', 'COMUNAEXPORTADORPPAL', 'CODIGORUTEXPSEC', 'NRO_EXPORTADOR_SEC', 'PORCENTAJEEXPSECUNDARIO', 'COMUNAEXPSECUNDARIO', 'PUERTOEMB', 'GLOSAPUERTOEMB', 'REGIONORIGEN', 'TIPOCARGA', 'VIATRANSPORTE', 'PUERTODESEMB', 'GLOSAPUERTODESEMB', 'PAISDESTINO', 'GLOSAPAISDESTINO', 'NOMBRECIATRANSP', 'PAISCIATRANSP', 'RUTCIATRANSP', 'DVRUTCIATRANSP', 'NOMBREEMISORDOCTRANSP', 'RUTEMISOR', 'DVRUTEMISOR', 'CODIGOTIPOAUTORIZA', 'NUMEROINFORMEEXPO', 'DVNUMEROINFORMEEXP', 'FECHAINFORMEEXP', 'MONEDA', 'MODALIDADVENTA', 'CLAUSULAVENTA', 'FORMAPAGO', 'VALORCLAUSULAVENTA', 'COMISIONESEXTERIOR', 'OTROSGASTOS', 'VALORLIQUIDORETORNO', 'NUMEROREGSUSP', 'ADUANAREGSUSP', 'PLAZOVIGENCIAREGSUSP', 'TOTALITEM', 'TOTALBULTOS', 'PESOBRUTOTOTAL', 'TOTALVALORFOB', 'VALORFLETE', 'CODIGOFLETE', 'VALORSEGURO', 'CODIGOSEG', 'VALORCIF', 'NUMEROPARCIALIDAD', 'TOTALPARCIALES', 'PARCIAL', 'OBSERVACION', 'NUMERODOCTOCANCELA', 'FECHADOCTOCANCELA', 'TIPODOCTOCANCELA', 'PESOBRUTOCANCELA', 'TOTALBULTOSCANCELA', 'NUMEROITEM', 'NOMBRE', 'ATRIBUTO1', 'ATRIBUTO2', 'ATRIBUTO3', 'ATRIBUTO4', 'ATRIBUTO5', 'ATRIBUTO6', 'CODIGOARANCEL', 'UNIDADMEDIDA', 'CANTIDADMERCANCIA', 'FOBUNITARIO', 'FOBUS', 'CODIGOOBSERVACION1', 'VALOROBSERVACION1', 'GLOSAOBSERVACION1', 'CODIGOOBSERVACION2', 'VALOROBSERVACION2', 'GLOSAOBSERVACION2', 'CODIGOOBSERVACION3', 'VALOROBSERVACION3', 'GLOSAOBSERVACION3', 'PESOBRUTOITEM',

 'FECHAACEPT_x', 'NUMEROBULTO', 'TIPOBULTO', 'CANTIDADBULTO', 'IDENTIFICACIONBULTO',
 'FECHAACEPT_y', 'NSECDOCTRANSP', 'NUMERODOCTRANSP', 'FECHADOCTRANSP', 'NOMBRENAVE', 'NUMEROVIAJE']


for year, files in file_to_complement.items():
    for file in files:
        main_file = file + ".txt"
        bultos_file = file + " Bultos.txt"
        transporte_file = file + " Transporte.txt"

        bultos_filename = read_path + os.sep + year + os.sep + bultos_file
        transporte_filename = read_path + os.sep + year + os.sep + transporte_file
        main_filename = complement_path + os.sep + year + os.sep + main_file

        bultos = pd.read_csv(bultos_filename, names=columns_bultos, header=None,
                             sep=";", encoding="latin1")

        transporte = pd.read_csv(transporte_filename, names=columns_transporte,
                                 header=None, sep=";", encoding="latin1")

        file = pd.read_csv(main_filename, names=columns_file, header=None,
                           sep=";", encoding="latin1")

        final = pd.merge(file,
                         pd.merge(bultos, transporte, on="NUMEROIDENT"),
                         on="NUMEROIDENT")

        final.to_csv(main_filename, sep=";", encoding="latin1", header=None, index=False)


print("Export filtered")




