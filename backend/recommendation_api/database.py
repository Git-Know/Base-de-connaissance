from pymongo import MongoClient

def get_collections():
    client = MongoClient("mongodb://mongodb:27017/")
    # client = MongoClient("mongodb://localhost:27017/")
    db = client["maBase"]
    return {
        "projects": db["projects"],
        "contributors": db["contributors"],
        "matching": db["matching"],
        "assignments": db["assignments"]
    }
