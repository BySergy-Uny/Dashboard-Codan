from pymongo import MongoClient
from tools.setup_dotenv import configuration_files

def get_connection():
    SECURE_URL_CONNECTION = configuration_files.MONGO_URL
    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(SECURE_URL_CONNECTION)
    print("[+] Client connected !")
    # Create the database for our example (we will use the same database throughout the tutorial
    return client

client = get_connection()