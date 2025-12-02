# pip install firebase-admin

from typing import Optional, Dict, Any
import firebase_admin
from firebase_admin import credentials, auth, initialize_app

def _init_firebase(credential_path: Optional[str] = None) -> None:
    """
    Initialize Firebase Admin once. If credential_path is provided,
    it should point to a service-account JSON. Otherwise uses ADC.
    """
    if firebase_admin._apps:
        return
    if credential_path:
        cred = credentials.Certificate(credential_path)
    else:
        cred = credentials.ApplicationDefault()
    initialize_app(cred)

def set_user_claims(uid: str, is_admin: bool, credential_path: Optional[str] = None,
                    merge: bool = True) -> Dict[str, Any]:
    """
    Set custom claims for the given user:
      - isAdmin: bool
      - userId : string (same as uid)

    Args:
        uid: The Firebase Auth UID.
        is_admin: Whether the user is an admin.
        credential_path: Optional path to a service account JSON.
                         If omitted, uses Application Default Credentials.
        merge: If True, merges with existing claims instead of overwriting.

    Returns:
        The claims that were set on the user.
    """
    _init_firebase(credential_path)
    print("Credentials initialized.", credential_path)
    # New claims we want to ensure
    settings = {"isAdmin": bool(is_admin), "userId": uid}

    if merge:
        user = auth.get_user(uid)
        merged = dict(user.custom_claims or {})
        merged.update(settings)
        claims_to_set = merged
    else:
        claims_to_set = settings

    auth.set_custom_user_claims(uid, claims_to_set)
    return claims_to_set

if __name__ == "__main__":
    # Example usage:
    credentials_local_path = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json" #Dev account
    #credentials_local_path = "/Users/marcelgleich/Desktop/Software/Firebase_Service//gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"  # Use Application Default Credentials (ADC)
    uid = "4qXpgdIK0vTg7wThQ89OHWMUlf13"

    claims = set_user_claims(uid, is_admin=True, credential_path=credentials_local_path)  # uses ADC
    # claims = set_user_claims("some-uid-123", is_admin=False, credential_path="service-account.json")
    print("Updated claims:", claims)
    user = auth.get_user(uid)
    print("Custom claims:", user.custom_claims)  # {'isAdmin': True, 'userId': '...
