import os
import pandas
from filter_file import filter_file

cwd = os.getcwd()
chunksize = 10 ** 3

columns =\
    ["TPO_DOCTO", "ADU", "FORM", "FECVENCI", "CODCOMUN", "NUM_UNICO_IMPORTADOR",
     "CODPAISCON", "DESDIRALM", "CODCOMRS", "ADUCTROL", "NUMPLAZO", "INDPARCIAL",
     "NUMHOJINS", "TOTINSUM", "CODALMA", "NUM_RS", "FEC_RS", "ADUA_RS",
     "NUMHOJANE", "NUM_SEC", "PA_ORIG", "PA_ADQ", "VIA_TRAN", "TRANSB",
     "PTO_EMB", "PTO_DESEM", "TPO_CARGA", "ALMACEN", "FEC_ALMAC", "FECRETIRO",
     "NU_REGR", "ANO_REG", "CODVISBUEN", "NUMREGLA", "NUMANORES", "CODULTVB",
     "PAGO_GRAV", "FECTRA", "FECACEP", "GNOM_CIA_T", "CODPAISCIA", "NUMRUTCIA",
     "DIGVERCIA", "NUM_MANIF", "NUM_MANIF1", "NUM_MANIF2", "FEC_MANIF",
     "NUM_CONOC", "FEC_CONOC", "NOMEMISOR", "NUMRUTEMI", "DIGVEREMI",
     "GREG_IMP", "REG_IMP", "BCO_COM", "CODORDIV", "FORM_PAGO", "NUMDIAS",
     "VALEXFAB", "MONEDA", "MONGASFOB", "CL_COMPRA", "TOT_ITEMS", "FOB",
     "TOT_HOJAS", "COD_FLE", "FLETE", "TOT_BULTOS", "COD_SEG", "SEGURO",
     "TOT_PESO", "CIF", "NUM_AUT", "FEC_AUT", "GBCOCEN", "ID_BULTOS",
     "TPO_BUL1", "CANT_BUL1", "TPO_BUL2", "CANT_BUL2", "TPO_BUL3", "CANT_BUL3",
     "TPO_BUL4", "CANT_BUL4", "TPO_BUL5", "CANT_BUL5", "TPO_BUL6", "CANT_BUL6",
     "TPO_BUL7", "CANT_BUL7", "TPO_BUL8", "CANT_BUL8", "CTA_OTRO", "MON_OTRO",
     "CTA_OTR1", "MON_OTR1", "CTA_OTR2", "MON_OTR2", "CTA_OTR3", "MON_OTR3",
     "CTA_OTR4", "MON_OTR4", "CTA_OTR5", "MON_OTR5", "CTA_OTR6", "MON_OTR6",
     "CTA_OTR7", "MON_OTR7", "MON_178", "MON_191", "FEC_501", "VAL_601",
     "FEC_502", "VAL_602", "FEC_503", "VAL_603", "FEC_504", "VAL_604",
     "FEC_505", "VAL_605", "FEC_506", "VAL_606", "FEC_507", "VAL_607", "TASA",
     "NCUOTAS", "ADU_DI", "NUM_DI", "FEC_DI", "MON_699", "MON_199", "NUMITEM",
     "DNOMBRE", "DMARCA", "DVARIEDAD", "DOTRO1", "DOTRO2","ATR_5", "ATR_6",
     "SAJU_ITEM", "AJU_ITEM", "CANT_MERC", "MERMAS", "MEDIDA", "PRE_UNIT",
     "ARANC_ALA", "NUMCOR", "NUMACU", "CODOBS1", "DESOBS1", "CODOBS2",
     "DESOBS2", "CODOBS3","DESOBS3", "CODOBS4", "DESOBS4", "ARANC_NAC",
     "CIF_ITEM", "ADVAL_ALA", "ADVAL", "VALAD", "OTRO1", "CTA1", "SIGVAL1",
     "VAL1", "OTRO2", "CTA2", "SIGVAL2", "VAL2", "OTRO3", "CTA3", "SIGVAL3",
     "VAL3", "OTRO4", "CTA4", "SIGVAL4","VAL4"
     ]

# Year and month to filter
file_to_filter = {
    "2017": ["Enero", "Febrero", "Marzo", "Abril", "Mayo"]
}

# Create filter folder
if not os.path.exists(cwd + os.sep + "Filter_Data"):
    os.makedirs(cwd + os.sep + "Filter_Data")

# Create import folder
if not os.path.exists(cwd + os.sep + "Filter_Data" + os.sep + "import"):
    os.makedirs(cwd + os.sep + "Filter_Data" + os.sep + "import")

# Import data
read_path = cwd + os.sep + "Original_Data" + os.sep + "import"
write_path = cwd + os.sep + "Filter_Data" + os.sep + "import"

for year, files in file_to_filter.items():
    for i in range(len(files)):
        archive = "{} - {}.txt".format(str(i+1).zfill(2), files[i])
        filename = read_path + os.sep + year + os.sep + archive
        filename_out = write_path + os.sep + year + os.sep + archive

        if not os.path.exists(write_path + os.sep + year):
            os.makedirs(write_path + os.sep + year)

        with open(filename_out, "w") as new_file:
            print(filename)
            for chunk in pandas.read_csv(filename, chunksize=chunksize,
                                         names=columns, header=None,
                                         sep=";", encoding="latin1"):
                filter_file(chunk, new_file)
                break
