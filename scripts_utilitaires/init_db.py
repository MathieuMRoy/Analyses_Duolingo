from duolingo_analyzer.discovery import initialiser_cibles
from duolingo_analyzer.config import TARGET_USERS_FILE
import os

if __name__ == "__main__":
    print("🚀 Reprise/Démarrage de la création de la base de données (3000 utilisateurs)...")
    initialiser_cibles()
    print("\n✅ Base de données terminée !")
