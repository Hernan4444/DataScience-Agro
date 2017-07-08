import os
import pandas as pd
from filter_file import filter_export_file

columns =\
    ["FECHAACEPT", "NUMEROIDENT", "ADUANA", "TIPOOPERACION",
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

file_to_filter = {
    "2017": ["01 - Enero"]
}

cwd = os.getcwd()

# Create filter folder
if not os.path.exists(cwd + os.sep + "Filter_Data"):
    os.makedirs(cwd + os.sep + "Filter_Data")


# Create import folder
if not os.path.exists(cwd + os.sep + "Filter_Data" + os.sep + "export"):
    os.makedirs(cwd + os.sep + "Filter_Data" + os.sep + "export")

chunksize = 10 ** 3

# Import data
read_path = cwd + os.sep + "Original_Data" + os.sep + "export"
write_path = cwd + os.sep + "Filter_Data" + os.sep + "export"

for year, files in file_to_filter.items():
    for file in files:
        archive = file + ".txt"
        filename = read_path + os.sep + year + os.sep + archive
        filename_out = write_path + os.sep + year + os.sep + archive

        if not os.path.exists(write_path + os.sep + year):
            os.makedirs(write_path + os.sep + year)

        with open(filename_out, "w") as new_file:
            for chunk in pd.read_csv(filename, chunksize=chunksize,
                                     names=columns, header=None,
                                     sep=";", encoding="latin1"):
                filter_export_file(chunk, new_file)

print("Export filtered")
