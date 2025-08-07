from pymongo import MongoClient

try:
    # Connexion à MongoDB sur le port 27018
    client = MongoClient("mongodb://localhost:27017/")

    # Sélection de la base et de la collection
    db = client["test_database"]
    collection = db["test_collection"]

    # Insertion d’un document de test
    result = collection.insert_one({"message": "Test réussi 🎉", "source": "test_mongo.py"})
    print("✅ Document inséré avec l’ID :", result.inserted_id)

except Exception as e:
    print("❌ Erreur de connexion ou d'insertion :", e)
