# pip install firebase-admin

import argparse
from typing import Optional
import firebase_admin
from firebase_admin import credentials, auth, initialize_app

# Firebase credentials configuration
DEV_CREDENTIALS_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json"
PROD_CREDENTIALS_PATH = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json"


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


def set_user_password(
    uid: str,
    new_password: str,
    credential_path: Optional[str] = None,
    environment: str = "dev"
) -> auth.UserRecord:
    """
    Set or overwrite the password for a Firebase user.

    Args:
        uid: The Firebase Auth UID of the user.
        new_password: The new password to set for the user.
        credential_path: Optional path to a service account JSON.
                         If omitted, uses environment to select default path.
        environment: Either "dev" or "prod" to select default credentials.
                    Only used if credential_path is not provided.

    Returns:
        The updated UserRecord.

    Raises:
        auth.UserNotFoundError: If the user with the given UID doesn't exist.
        ValueError: If the password is invalid or other validation fails.
    """
    # Select credential path if not provided
    if not credential_path:
        if environment.lower() == "prod":
            credential_path = PROD_CREDENTIALS_PATH
        else:
            credential_path = DEV_CREDENTIALS_PATH

    _init_firebase(credential_path)
    print(f"Credentials initialized: {credential_path}")
    print(f"Environment: {environment.upper()}")

    try:
        # Verify user exists first
        user = auth.get_user(uid)
        print(f"Found user: {user.email} (UID: {user.uid})")

        # Update the user's password
        updated_user = auth.update_user(uid, password=new_password)
        print(f"✓ Password successfully updated for user: {updated_user.email}")
        
        return updated_user

    except auth.UserNotFoundError:
        print(f"✗ Error: User with UID '{uid}' not found.")
        raise
    except ValueError as e:
        print(f"✗ Error: Invalid password or validation failed: {e}")
        raise
    except Exception as e:
        print(f"✗ Error updating password: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Set or overwrite the password for a Firebase user."
    )
    parser.add_argument(
        "--uid",
        required=True,
        help="Firebase Auth UID of the user.",
    )
    parser.add_argument(
        "--password",
        required=True,
        help="New password to set for the user.",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment: dev or prod (default: dev).",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("SET USER PASSWORD")
    print("=" * 60)
    print(f"User UID: {args.uid}")
    print(f"Environment: {args.env.upper()}")
    print("=" * 60)
    print()

    try:
        updated_user = set_user_password(
            uid=args.uid,
            new_password=args.password,
            environment=args.env,
        )
        print()
        print("=" * 60)
        print("SUCCESS")
        print("=" * 60)
        print(f"User email: {updated_user.email}")
        print(f"User UID: {updated_user.uid}")
        print("Password has been updated.")
        print()
        print("Credentials set:")
        print(f"  User ID: {updated_user.uid}")
        print(f"  Password: {args.password}")
        print("=" * 60)
    except Exception as e:
        print()
        print("=" * 60)
        print("FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        print("=" * 60)

