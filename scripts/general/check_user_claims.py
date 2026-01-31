# pip install firebase-admin

from typing import Optional, Dict, Any
import json
import firebase_admin
from firebase_admin import credentials, auth, initialize_app

# Configure these values:
# SERVICE_ACCOUNT_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"  # Dev account
SERVICE_ACCOUNT_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"  # Prod account

USER_ID = "k9eSpYCsEYUlhAUYM073qvQhOSH2"  # Set the user ID to check


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


def check_user_claims(uid: str, credential_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Check custom claims for the given user.

    Args:
        uid: The Firebase Auth UID.
        credential_path: Optional path to a service account JSON.
                         If omitted, uses Application Default Credentials.

    Returns:
        Dictionary containing user information and claims.
    """
    _init_firebase(credential_path)
    print(f"Credentials initialized: {credential_path or 'Application Default Credentials'}")
    
    try:
        user = auth.get_user(uid)
        
        # Get all custom claims (this includes all claims like isAdmin, userId, etc.)
        custom_claims = user.custom_claims or {}
        
        result = {
            "uid": user.uid,
            "email": user.email,
            "email_verified": user.email_verified,
            "display_name": user.display_name,
            "disabled": user.disabled,
            "custom_claims": custom_claims,
            "creation_timestamp": user.user_metadata.creation_timestamp,
            "last_sign_in_timestamp": user.user_metadata.last_sign_in_timestamp,
            "phone_number": user.phone_number,
            "photo_url": user.photo_url,
            "provider_data": [{"provider_id": p.provider_id, "uid": p.uid, "email": p.email} for p in user.provider_data] if user.provider_data else [],
        }
        
        return result
    except auth.UserNotFoundError:
        print(f"Error: User with UID '{uid}' not found.")
        raise
    except Exception as e:
        print(f"Error retrieving user: {e}")
        raise


if __name__ == "__main__":
    print(f"Checking claims for user: {USER_ID}")
    print(f"Using service account: {SERVICE_ACCOUNT_PATH}\n")
    
    user_info = check_user_claims(USER_ID, credential_path=SERVICE_ACCOUNT_PATH)
    
    print("=" * 60)
    print("USER INFORMATION")
    print("=" * 60)
    print(f"UID:                {user_info['uid']}")
    print(f"Email:              {user_info['email']}")
    print(f"Email Verified:     {user_info['email_verified']}")
    print(f"Display Name:       {user_info['display_name']}")
    print(f"Disabled:           {user_info['disabled']}")
    print(f"Created:            {user_info['creation_timestamp']}")
    print(f"Last Sign In:       {user_info['last_sign_in_timestamp']}")
    print()
    print("=" * 60)
    print("CUSTOM CLAIMS (All Claims)")
    print("=" * 60)
    if user_info['custom_claims']:
        # Display as formatted JSON for better readability
        print(json.dumps(user_info['custom_claims'], indent=2, default=str))
        print()
        # Also display as key-value pairs
        print("Key-Value Format:")
        for key, value in user_info['custom_claims'].items():
            print(f"  {key}: {value} ({type(value).__name__})")
    else:
        print("  (no custom claims)")
    print("=" * 60)
    
    # Additional user info
    if user_info.get('phone_number'):
        print(f"Phone Number:       {user_info['phone_number']}")
    if user_info.get('photo_url'):
        print(f"Photo URL:          {user_info['photo_url']}")
    if user_info.get('provider_data'):
        print(f"Provider Data:       {len(user_info['provider_data'])} provider(s)")
        for provider in user_info['provider_data']:
            print(f"  - {provider['provider_id']}: {provider.get('email', provider.get('uid', 'N/A'))}")
