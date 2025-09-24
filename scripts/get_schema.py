import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firestore once
cred = credentials.Certificate("/Users/marcelgleich/Desktop/Software/Firesbase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-1b9e04b746.json")  # path to your service account JSON
firebase_admin.initialize_app(cred)
db = firestore.client()


def infer_type(value):
    """Infer a simple type string from Firestore value."""
    if isinstance(value, bool):
        return "boolean"
    elif isinstance(value, int) or isinstance(value, float):
        return "number"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, dict):
        return {k: infer_type(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [infer_type(value[0])] if value else ["unknown"]
    else:
        return type(value).__name__  # fallback, e.g. Timestamp, GeoPoint, etc.


def get_schema(collection_name):
    """Pull one document from collection and return its schema."""
    docs = db.collection(collection_name).limit(1).stream()
    for doc in docs:
        data = doc.to_dict()
        return {field: infer_type(value) for field, value in data.items()}
    return {}  # empty if no docs in collection


if __name__ == "__main__":
    collections = ["customers", "hits", "links", "businesses", "campaigns"]
    schema = {}
    for col in collections:
        schema[col] = get_schema(col)

    import json
    print(json.dumps(schema, indent=2))
