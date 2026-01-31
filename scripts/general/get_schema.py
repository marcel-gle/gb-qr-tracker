import firebase_admin
from firebase_admin import credentials, firestore
from typing import Dict, Any, Optional

# Initialize Firestore once
cred = credentials.Certificate("/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json")  # path to your service account JSON
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


def get_document_schema(doc_ref):
    """Get schema from a single document."""
    doc = doc_ref.get()
    if not doc.exists:
        return {}
    data = doc.to_dict()
    if not data:
        return {}
    return {field: infer_type(value) for field, value in data.items()}


def get_subcollections_schema(doc_ref, max_depth: int = 2, current_depth: int = 0):
    """Get schemas for all subcollections of a document, recursively."""
    if current_depth >= max_depth:
        return {}
    
    subcollections_schema = {}
    try:
        # List all subcollections for this document
        subcollections = doc_ref.collections()
        for subcol in subcollections:
            subcol_name = subcol.id
            # Get one document from the subcollection to infer schema
            subcol_docs = subcol.limit(1).stream()
            subcol_schema = {}
            subcol_fields = {}
            
            for subdoc in subcol_docs:
                subcol_data = subdoc.to_dict()
                if subcol_data:
                    subcol_fields = {field: infer_type(value) for field, value in subcol_data.items()}
                # Recursively check for nested subcollections
                nested_subcols = get_subcollections_schema(subdoc.reference, max_depth, current_depth + 1)
                if nested_subcols:
                    subcol_schema = {
                        "fields": subcol_fields,
                        "subcollections": nested_subcols
                    }
                else:
                    subcol_schema = subcol_fields
                break  # Only need one document to infer schema
            
            if subcol_schema:
                subcollections_schema[subcol_name] = subcol_schema
    except Exception as e:
        # Some documents might not have accessible subcollections
        pass
    
    return subcollections_schema


def get_schema(collection_name):
    """Pull one document from collection and return its schema, including subcollections."""
    docs = db.collection(collection_name).limit(1).stream()
    for doc in docs:
        data = doc.to_dict()
        fields_schema = {field: infer_type(value) for field, value in data.items()} if data else {}
        
        # Get subcollections schema
        subcollections_schema = get_subcollections_schema(doc.reference)
        
        if subcollections_schema:
            return {
                "fields": fields_schema,
                "subcollections": subcollections_schema
            }
        else:
            return fields_schema
    return {}  # empty if no docs in collection


if __name__ == "__main__":
    collections = ["customers", "hits", "links", "businesses", "campaigns"]
    schema = {}
    for col in collections:
        print(f"Processing collection: {col}...", file=__import__("sys").stderr)
        schema[col] = get_schema(col)

    import json
    print(json.dumps(schema, indent=2))
