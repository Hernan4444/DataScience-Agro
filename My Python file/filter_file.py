from _io import TextIOWrapper
from pandas.core.frame import DataFrame


def filter_import_file(lines: DataFrame, file: TextIOWrapper):
    for index, row in lines.iterrows():
        aranc_ala = row["ARANC_ALA"]
        aranc_nac = row["ARANC_NAC"]

        if 6000000 <= aranc_ala <= 69999999 and\
           6000000 <= aranc_nac <= 69999999:

            file.write(";".join([str(_) for _ in row]) + "\n")


def filter_export_file(lines: DataFrame, file: TextIOWrapper):
    for index, row in lines.iterrows():
        aranc_ala = row["CODIGOARANCEL"]

        if 6000000 <= aranc_ala <= 69999999:
            file.write(";".join([str(_) for _ in row]) + "\n")
