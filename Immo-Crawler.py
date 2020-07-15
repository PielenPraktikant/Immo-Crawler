# Pielen & Partner
# ImmobilienScout24 Web Crawler
# Erstellt am 15.07.2020
# von Max Hilgenberg


import bs4 as bs
import urllib.request
import time
from datetime import datetime
import pandas as pd
import json

for seite in range(1,100):
    
    print("Seite " + str(seite) + " wird jetzt von dem Crawler durchsucht.")

    df = pd.DataFrame()
    l=[]

    try:
        
        soup = bs.BeautifulSoup(urllib.request.urlopen("https://www.immobilienscout24.de/Suche/S-2/P-"+str(seite)+"/Haus-Kauf").read(),'lxml')
        print("Crawler ist jetzt auf dieser Webseite:\n        "+"https://www.immobilienscout24.de/Suche/S-2/P-"+str(seite)+"/Haus-Kauf")
        for paragraph in soup.find_all("a"):

            if r"/expose/" in str(paragraph.get("href")):
                l.append(paragraph.get("href").split("#")[0])

            l = list(set(l))

        for item in l:

            try:

                soup = bs.BeautifulSoup(urllib.request.urlopen('https://www.immobilienscout24.de'+item).read(),'lxml')

                data = pd.DataFrame(json.loads(str(soup.find_all("script")).split("keyValues = ")[1].split("}")[0]+str("}")),index=[str(datetime.now())])
                #Das ist die Suche für die Makler Webseite:
                Makler_Webseite = soup.find("a", id="is24-expose-realtor-box-homepage")

                #print(Makler_Webseite)
                data["Makler"] = str(Makler_Webseite)
                data["Makler"].replace(regex=True,inplace=True,to_replace=r'<a class="font-regular is24-external" data-is24-realtor-home-page-link-reporting="" href="',value=r'')
                data["Makler"].replace(regex=True,inplace=True,to_replace=r'" id="is24-expose-realtor-box-homepage">',value=r'||')
                data["Makler"].replace(regex=True,inplace=True,to_replace=r'</a>',value=r'')
                data["URL"] = "https://www.immobilienscout24.de/" + str(item)

                #beschreibung = []
                
                #for i in soup.find_all("pre"):
                 #   beschreibung.append(i.text)

                #data["beschreibung"] = str(beschreibung)

                df = df.append(data)
                #df  = df.filter('obj_zipCode')
            except Exception as e: 
                print(str(datetime.now())+": " + str(e))
                l = list(filter(lambda x: x != item, l))
                print("ID " + str(item) + " entfernt.")
        print("Exportiert CSV")
        df.to_csv("./Rohdaten/"+str(datetime.now())[:19].replace(":","").replace(".","")+".csv",sep=";",decimal=",",encoding = "utf-8",index_label="timestamp")     

        print("Loop " + str(seite) + " endet.\n")
        
    except Exception as e: 
        print(str(datetime.now())+": " + str(e))

print("Webseiten wurden durchsucht. Jetzt zum zusammenfügen!\n")


import pandas as pd
import os

df = pd.DataFrame()

count=1

for i in os.listdir("./Rohdaten/"):
    print(str(count)+". Datei: "+str(i))
    count+=1
    df = df.append(pd.read_csv("./Rohdaten/" + str(i),sep=";",encoding="utf-8",decimal=","))
    

    df.shape
    data["Makler"].replace(regex=True,inplace=True,to_replace=r'<a class="font-regular is24-external" data-is24-realtor-home-page-link-reporting="" href="',value=r'')
    data["Makler"].replace(regex=True,inplace=True,to_replace=r'" id="is24-expose-realtor-box-homepage">',value=r'||')
    data["Makler"].replace(regex=True,inplace=True,to_replace=r'</a>',value=r'')
    df = df.drop_duplicates(subset="URL")

    df.shape

    df.to_csv("./Mitte/Komplett.csv",sep=";",encoding="utf-8",decimal=",")
    
    
    print("Nun zum Final Step!")
    
    
    
    #import pandas as pd
import os

df = pd.DataFrame()

count=1

for i in os.listdir("./Mitte/"):
    print("Vorgang läuft")
    #print(str(count)+". Datei: "+str(i) "")
    count+=1
    df = df.append(pd.read_csv("./Mitte/" + str(i),sep=";",encoding="utf-8",decimal=","))
    df.shape
    
    df = df.drop('obj_regio1', axis=1)
    df = df.drop('obj_heatingType', axis=1)
    df = df.drop('obj_telekomTvOffer', axis=1)
    df = df.drop('ga_cd_customer_group', axis=1)
    df = df.drop('obj_cId', axis=1)
    df = df.drop('obj_plotAreaRange', axis=1)
    df = df.drop('obj_picture', axis=1)
    df = df.drop('obj_picturecount', axis=1)
    df = df.drop('obj_pricetrend', axis=1)
    df = df.drop('obj_telekomUploadSpeed', axis=1)
    df = df.drop('obj_lotArea', axis=1)
    df = df.drop('obj_telekomTrackingGroup', axis=1)
    df = df.drop('obj_usableArea', axis=1)
    df = df.drop('obj_telekomInternetTechnology', axis=1)
    df = df.drop('obj_telekomInternetType', axis=1)
    df = df.drop('obj_pricetrendbuy', axis=1)
    df = df.drop('obj_scoutId', axis=1)
    df = df.drop('obj_noParkSpaces', axis=1)
    df = df.drop('obj_firingTypes', axis=1)
    df = df.drop('obj_ExclusiveExpose', axis=1)
    df = df.drop('obj_telekomInternetProductName', axis=1)
    df = df.drop('obj_courtage', axis=1)
    #df = df.drop('geo_bln', axis=1)
    df = df.drop('obj_cellar', axis=1)
    df = df.drop('obj_purchasePriceRange', axis=1)
    df = df.drop('obj_yearConstructedRange', axis=1)
    df = df.drop('obj_houseNumber', axis=1)
    df = df.drop('obj_pricetrendrent', axis=1)
    df = df.drop('obj_livingSpace', axis=1)
    #df = df.drop('geo_krs', axis=1)
    df = df.drop('obj_condition'	, axis=1)
    df = df.drop('obj_interiorQual', axis=1)
    df = df.drop('ga_cd_cxp_historicallisting', axis=1)
    df = df.drop('obj_telekomDownloadSpeed', axis=1)
    df = df.drop('obj_street', axis=1)
    df = df.drop('obj_telekomInternetUrlMobile', axis=1)
    df = df.drop('obj_telekomInternetUrl', axis=1)
    df = df.drop('obj_streetPlain', axis=1)
    df = df.drop('geo_plz', axis=1)
    df = df.drop('obj_constructionPhase', axis=1)
    df = df.drop('obj_groupnumber', axis=1)
    df = df.drop('obj_ityp', axis=1)
    df = df.drop('obj_telekomHdTelephone', axis=1)
    df = df.drop('geo_land', axis=1)
    df = df.drop('ga_cd_via', axis=1)
    df = df.drop('obj_telekomInternet'	, axis=1)
    df = df.drop('obj_immotype', axis=1)
    df = df.drop('obj_telekomInternetServices', axis=1)
    df = df.drop('obj_telekomInternetProductAvailable', axis=1)
    df = df.drop('obj_cwId', axis=1)
    df = df.drop('ga_cd_test_cxp_expose', axis=1)
    df = df.drop('ga_cd_application_requirements', axis=1)
    #df = df.drop('obj_buildingType', axis=1)
    df = df.drop('obj_noRoomsRange'	, axis=1)
    df = df.drop('ga_cd_maillead_default_shown', axis=1)
    df = df.drop('evt_count_pm_sig', axis=1)
    df = df.drop('obj_barrierFree', axis=1)
    df = df.drop('obj_international', axis=1)
    df = df.drop('obj_regio3', axis=1)
    df = df.drop('obj_livingSpaceRange', axis=1)
    df = df.drop('obj_regio2', axis=1)
    #df = df.drop('beschreibung', axis=1)
    df = df.drop('obj_galleryAd', axis=1)
    df = df.drop('obj_numberOfFloors', axis=1)
    df = df.drop('obj_energyType', axis=1)
    df = df.drop('obj_energyEfficiencyClass', axis=1)
    df = df.drop('obj_thermalChar', axis=1)
    #df = df.drop('obj_lastRefurbish', axis=1)
    df = df.drop('ga_cd_developer_virtualreality', axis=1)
    df = df.drop('obj_telekomHybridUploadSpeed'	, axis=1)
    df = df.drop('obj_telekomHybridDownloadSpeed', axis=1)
    df = df.drop('obj_regio4', axis=1)
    df = df.drop('obj_project_id', axis=1)
    df = df.drop('obj_nbp', axis=1)
    df = df.drop('ga_cd_via_qualified', axis=1)
    df = df.drop('obj_rented', axis=1)
    df = df.drop('geo_ot', axis=1)
    df = df.drop('geo_bg', axis=1)
    df.columns.values[0] = 'Immobilienanzahl'   
    df.columns.values[1] = 'Timestamp'
    df.columns.values[2] = 'Neubau Y|N'   
    df.columns.values[3] = 'Baujahr'   
    df.columns.values[4] = 'Bundesland'   
    df.columns.values[5] = 'Umkreis der Immobilie'   
    df.columns.values[6] = 'Postleitzahl'   
    df.columns.values[7] = 'Zimmer'   
    df.columns.values[8] = 'Kaufpreis'   
    df.columns.values[9] = 'Hauskategorie'   
    df.columns.values[10] = 'Object Nummer'   
    df.columns.values[11] = 'Makler              '   
   #df.columns.values[12] = 'ImmobilienScout24 Webseite'   
    df.columns.values[13] = 'Letzte Renovierung'  
 
    
    df = df.drop_duplicates(subset="URL")

    df.shape
    df.to_csv("./Final.csv",sep=";",encoding="utf-8",decimal=",")
    df.to_csv("./Final/Final.csv",sep=";",encoding="utf-8",decimal=",")
    
    print("Nun ist es alles vorbei!!!\nBereit zur Untersuchung!")
    
