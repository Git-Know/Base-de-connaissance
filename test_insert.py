from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["maBase"]
projects = db["projects"]

# Insérer un document test
projects.insert_one({"repository": "test-repo", "summary": "Ceci est un test"})

print("Document inséré dans maBase.projects")
