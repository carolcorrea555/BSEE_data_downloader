from requests import get, head
from datetime import datetime
import zipfile as zf
import json
from io import BytesIO
from pathlib import Path
import pandas as pd

Path("Downloads").mkdir(parents=True, exist_ok=True)

try:
    last_modified = json.load(open("last_modified.json", "rt"))
except:
    last_modified = {}

class BSEE_data:
    def __init__(self, label, type, url, dataFrame = pd.DataFrame()):
        self.label = label
        self.type = type
        self.url = url
        self.dataFrame = dataFrame

    def _parse_OGOR_A(self):
        txt = open("Downloads/" + self.label + ".txt").read()

        txt = txt.split("\n")
        dataFrame = []
        col = ["lease_number", "production_date", "production_code",
               "monthly_oil_volume", "monthly_gas_volume", "operator_num"]

        for lin in txt:
            dataFrame += [[lin[0:7], lin[13:13+6], lin[21:21+1],
                              lin[22:22+9], lin[31:31+9], lin[71:71+5]]]

        dataFrame = pd.DataFrame(dataFrame, columns=col, dtype="object")

        return dataFrame

    def _parse_company(self):
        txt = open("Downloads/" + self.label + ".txt").read()

        txt = txt.split("\n")
        dataFrame = []
        col = ["mms_company_num", "bus_asc_name"]

        for lin in txt:
            if [lin[0:5], lin[13:13+100]] in dataFrame:
                pass
            else:
                dataFrame += [[lin[0:5], lin[13:13+100]]]

        dataFrame = pd.DataFrame(dataFrame, columns=col, dtype="object")

        return dataFrame

    def _parse_lease(self):
        dat = open("Downloads/LSETAPE.DAT", "rt").read()

        dat = dat.split("\n")
        dataFrame = []
        col = ["lease_number", "lease_expiration_date", "block_max_water_depth",
                               "area_code"]

        for lin in dat:
            dataFrame += [[lin[0:7], lin[27:27+8], lin[154:154+5], lin[288:288+2]]]

        dataFrame = pd.DataFrame(dataFrame, columns=col,dtype="object")

        return dataFrame

    def populate(self):
        parse_dict = {"OGOR-A":self._parse_OGOR_A,
                      "Leasing":self._parse_lease,
                      "Company":self._parse_company}

        resp = head(self.url)

        try:
            if resp.headers["Last-Modified"] != last_modified[self.label]:
                raise

        except:
            resp = get(self.url)

            last_modified[self.label] = resp.headers["Last-Modified"]

            with zf.ZipFile(BytesIO(resp.content), mode="r") as archive:
                for filename in archive.namelist():
                    open("Downloads/"+filename, "wb").write(archive.read(filename))

        self.dataFrame = parse_dict[self.type]()

        

def OGOR_A_data(year):
    if year == datetime.now().year:
        label = "ogorafixed"
    else:
        label =  "ogora" + str(year) + "fixed"

    url = "https://www.data.bsee.gov/Production/Files/" + label + ".zip"

    return BSEE_data(label, "OGOR-A", url)

def lease_data():
    label = "lsetapefixed"
    url = "https://www.data.bsee.gov/Leasing/Files/lsetapefixed.zip"

    return BSEE_data(label, "Leasing", url)

def company_data():
    label = "compallfixed"
    url = "https://www.data.bsee.gov/Company/Files/compallfixed.zip"

    return BSEE_data(label, "Company", url)
    

yearList = range(2019,2021)
pdataList = [OGOR_A_data(year) for year in yearList]

ldata = lease_data()
cdata = company_data()

production = pd.DataFrame(columns=["lease_number", "production_date", "production_code",
               "monthly_oil_volume", "monthly_gas_volume", "operator_num"])

leases = pd.DataFrame(columns=["lease_number", "lease_expiration_date", "block_max_water_depth",
                               "area_code"])

companies = pd.DataFrame(columns=["mms_company_num", "bus_asc_name"])

for pdata in pdataList:
    pdata.populate()
    production = pd.concat([pdata.dataFrame,production], ignore_index=True)

ldata.populate()
leases = pd.concat([ldata.dataFrame,leases], ignore_index=True)

cdata.populate()
companies = pd.concat([cdata.dataFrame,companies], ignore_index=True)
    
production.to_csv("Downloads/production.csv")
leases.to_csv("Downloads/leases.csv")
companies.to_csv("Downloads/companies.csv")

json.dump(last_modified, open("last_modified.json", "wt"))


