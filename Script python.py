import pandas as pd
import requests
from datetime import datetime

today = datetime.today().strftime('%Y-%m-%d')                           #générer date du jour avce le bon format date
url_degradation = f'https://sc-e.fr/docs/degradations_{today}.csv'      #générer l'url du csv avec la date du jour
url_logs_vols = f'https://sc-e.fr/docs/logs_vols_{today}.csv'           #idem avec logs_vols


file_path_degradations = r"C:\Users\PC\Documents\PRO\PROJET 3\degradations_historique.csv"              #définir les chemins des fichiers existants pour enregistrer les df concaténés
file_path_logs_vols = r"C:\Users\PC\Documents\PRO\PROJET 3\logs_vols_historique.csv"                     #je suis repartie des fichiers MAJ au 07/06 
file_path_composants = r"C:\Users\PC\Documents\PRO\PROJET 3\composants_MAJ.csv"
file_path_aeronefs = r"C:\Users\PC\Documents\PRO\PROJET 3\aeronefs_MAJ.csv"


df_degradations = pd.read_csv(file_path_degradations)               #charger les df existants depuis des fichiers csv
df_logs_vols = pd.read_csv(file_path_logs_vols)
df_composants = pd.read_csv(file_path_composants)
df_aeronefs = pd.read_csv(file_path_aeronefs)


#Création d'une focntion pour récupérer le csv via l'url
def load_csv_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return pd.read_csv(url)
    except pd.errors.EmptyDataError:
        print(f"Le fichier CSV {url} est vide ou n'existe pas.")
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération du fichier {url} : {e}")
    return pd.DataFrame()

#A partir de la fonction, récuperer les fichiers à jour degradations et logs_vols
df_degradations_new = load_csv_from_url(url_degradation)
df_logs_vols_new = load_csv_from_url(url_logs_vols)

#On concatène les fichier récupérés avec le fichier déjà existant
df_degradations_concat = pd.concat([df_degradations, df_degradations_new], ignore_index=True)
df_logs_vols_concat = pd.concat([df_logs_vols, df_logs_vols_new], ignore_index=True)

#On supprime les éventuelles lignes en doublons (dans le cas où on charge deux fois un fichier)
df_degradations_concat.drop_duplicates(inplace=True)
df_logs_vols_concat.drop_duplicates(inplace=True)

#MAJ de la table composants, colonne taux_usure_actuel en reprenant les info de la colonne usure_nouvelle
for index, row in df_degradations_new.iterrows():
    compo_concerned = row['compo_concerned']
    usure_nouvelle = row['usure_nouvelle']
    df_composants.loc[df_composants['ref_compo'] == compo_concerned, 'taux_usure_actuel'] = usure_nouvelle


#Enregistrer les df modifiés dans les fichiers définis au début
df_composants.to_csv(file_path_composants, index=False)
df_degradations_concat.to_csv(file_path_degradations, index=False)
df_logs_vols_concat.to_csv(file_path_logs_vols, index=False)
df_aeronefs.to_csv(file_path_aeronefs, index=False)

print("Mise à jour des tables composants et logs terminée.")
