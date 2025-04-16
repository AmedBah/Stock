import os
import time
import pandas as pd
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta

# Connexion MongoDB
client = MongoClient(os.getenv("MONGO_URI"))
db = client["sika_finance"]
collection = db["historique_actions"]

def get_last_record_date():
    last_record = collection.find_one()
    if last_record:
        try:
            last_date_str = last_record["historique"][-1]["Date"]
            last_date = datetime.strptime(last_date_str, "%d/%m/%Y")
            return last_date
        except (ValueError, KeyError, IndexError) as e:
            print(f"Erreur de récupération de la dernière date : {e}")
            return None
    return None

# Définir les plages de dates
last_date = get_last_record_date()
if last_date:
    date_from = last_date + timedelta(days=1)
else:
    date_from = datetime.strptime("01/01/2025", "%d/%m/%Y")

date_to = datetime.now()
date_from_str = date_from.strftime("%d/%m/%Y")
date_to_str = date_to.strftime("%d/%m/%Y")

print(f"Récupération des données de {date_from_str} à {date_to_str}")

# Configuration du navigateur Selenium
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 10)

# Ouvrir une première page pour initialiser le select
driver.get("https://www.sikafinance.com/marches/historiques/SDSC.ci")
time.sleep(3)

select_element = wait.until(EC.presence_of_element_located((By.ID, "dpShares")))
select_html = Select(select_element)
options = [(opt.text.strip(), opt.get_attribute("value").strip())
           for opt in select_html.options if opt.get_attribute("value").strip()]

# Fonction pour convertir en float
def convert_to_float(val):
    if isinstance(val, str):
        val = val.replace(" ", "").replace(",", ".")
        try:
            return float(val)
        except ValueError:
            return val
    return val

# Parcourir toutes les actions
for nom_action, valeur in options:
    print(f"🔍 Traitement de {nom_action} ({valeur})...")

    driver.get(f"https://www.sikafinance.com/marches/historiques/{valeur}")
    time.sleep(3)

    try:
        datefrom_input = wait.until(EC.presence_of_element_located((By.ID, "datefrom")))
        dateto_input = driver.find_element(By.ID, "dateto")

        datefrom_input.clear()
        datefrom_input.send_keys(date_from_str)
        dateto_input.clear()
        dateto_input.send_keys(date_to_str)

        driver.find_element(By.ID, "btnChange").click()
        time.sleep(5)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        table = soup.find("table", id="tblhistos")

        if table:
            headers = [th.text.strip() for th in table.find_all("th")]
            rows = [[td.text.strip() for td in tr.find_all("td")] for tr in table.find_all("tr")[1:] if tr.find_all("td")]

            df = pd.DataFrame(rows, columns=headers)
            df["Action"] = nom_action

            # Conversion des colonnes numériques
            for col in df.columns:
                if col not in ["Date", "Action"]:
                    df[col] = df[col].apply(convert_to_float)

            data = df.to_dict(orient="records")

            collection.update_one(
                {"action": nom_action},
                {"$push": {"historique": {"$each": data}}},
                upsert=True
            )
            print(f"✅ {len(data)} lignes insérées pour {nom_action}")
        else:
            print(f"❌ Aucun tableau trouvé pour {nom_action}")
    except Exception as e:
        print(f"⚠️ Erreur pour {nom_action} : {e}")
    time.sleep(2)

driver.quit()
print("🚀 Scraping terminé et données enregistrées.")
