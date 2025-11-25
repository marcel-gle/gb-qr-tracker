"""
Cloud Function to list campaign files from Google Cloud Storage.

This function lists PDF templates, CSV lists, and JSON upload reports
from the storage paths: uploads/{env}/{uid}/{campaign_id}/source and /templates
"""

import json
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from google.cloud import storage
from flask import Request, jsonify
from typing import Any
import firebase_admin
from firebase_admin import auth, credentials
import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest


# Initialize Firebase Admin (only if not already initialized)
if not firebase_admin._apps:
    # Try to use application default credentials or service account
    try:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
    except Exception:
        # Fallback: try without explicit credentials (uses default)
        firebase_admin.initialize_app()


def verify_firebase_token(id_token: str) -> Dict[str, Any]:
    """Verify Firebase ID token and return decoded token."""
    try:
        decoded_token = auth.verify_id_token(id_token)
        return decoded_token
    except Exception as e:
        raise ValueError(f"Invalid token: {str(e)}")


def get_file_type(filename: str) -> Optional[str]:
    """Determine file type from extension."""
    ext = filename.lower().split('.')[-1] if '.' in filename else ''
    if ext == 'pdf':
        return 'pdf'
    elif ext == 'csv':
        return 'csv'
    elif ext == 'json':
        return 'json'
    return None


def list_campaign_files(request: Any) -> tuple:
    """
    Cloud Function entry point.
    
    Expected request body:
    {
        "campaignId": "uuid-string",
        "env": "dev" (optional, defaults to "dev"),
        "uid": "user-id" (optional, uses token uid if not provided)
    }
    
    Returns:
    {
        "files": [...],
        "source": [...],
        "templates": [...]
    }
    """
    # Handle CORS preflight
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    # Set CORS headers for actual response
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json'
    }
    
    try:
        # Verify authentication
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return (
                jsonify({'error': 'Missing or invalid Authorization header'}),
                401,
                cors_headers
            )
        
        id_token = auth_header.split('Bearer ')[1]
        decoded_token = verify_firebase_token(id_token)
        token_uid = decoded_token.get('uid')
        
        # Parse request body
        if not request.is_json:
            return (
                jsonify({'error': 'Request must be JSON'}),
                400,
                cors_headers
            )
        
        body = request.get_json()
        campaign_id = body.get('campaignId')
        env = body.get('env', 'dev')
        uid = body.get('uid', token_uid)
        
        if not campaign_id:
            return (
                jsonify({'error': 'Missing campaignId'}),
                400,
                cors_headers
            )
        
        # Verify user has access (user must own the campaign or be admin)
        # Note: You might want to add additional authorization checks here
        # For now, we'll just verify the uid matches or user is admin
        if uid != token_uid:
            # Check if user is admin (you can customize this check)
            custom_claims = decoded_token.get('isAdmin', False)
            if not custom_claims:
                return (
                    jsonify({'error': 'Unauthorized: Cannot access other user\'s files'}),
                    403,
                    cors_headers
                )
        
        # Get storage bucket from environment or use default
        storage_bucket_name = os.environ.get(
            'STORAGE_BUCKET',
            'gb-qr-tracker-dev.firebasestorage.app'
        )
        
        # Initialize Cloud Storage client
        storage_client = storage.Client()
        bucket = storage_client.bucket(storage_bucket_name)

        # Get credentials for signing URLs (ADC)
        credentials, _ = google.auth.default()
        credentials.refresh(GoogleAuthRequest())

        service_account_email = getattr(credentials, "service_account_email", None)
        access_token = credentials.token

        if not service_account_email:
            raise RuntimeError(
                "No service_account_email on ADC credentials; "
                "check the Cloud Function service account."
            )
        
        # Build storage paths
        base_path = f"uploads/{env}/{uid}/{campaign_id}"
        source_path = f"{base_path}/source"
        templates_path = f"{base_path}/templates"

        print(f"base_path: {base_path}")
        print(f"source_path: {source_path}")
        print(f"templates_path: {templates_path}")

        
        
        # List files from both folders
        all_files: List[Dict[str, Any]] = []
        source_files: List[Dict[str, Any]] = []
        template_files: List[Dict[str, Any]] = []
        
        def process_blobs(prefix: str, folder: str) -> List[Dict[str, Any]]:
            """List and process blobs in a given prefix."""
            files = []
            try:
                blobs = bucket.list_blobs(prefix=prefix)
                for blob in blobs:
                    if blob.name.endswith('/'):
                        continue

                    file_type = get_file_type(blob.name)
                    if not file_type:
                        continue

                    # Generate signed download URL (valid for 1 hour)
                    download_url = blob.generate_signed_url(
                        version="v4",  # explicit V4 signing
                        expiration=datetime.utcnow() + timedelta(hours=1),
                        method="GET",
                        service_account_email=service_account_email,
                        access_token=access_token,
                    )

                    filename = blob.name.split('/')[-1]

                    file_info = {
                        'name': filename,
                        'path': blob.name,
                        'size': blob.size,
                        'downloadUrl': download_url,
                        'type': file_type,
                        'folder': folder,
                        'updated': blob.updated.isoformat() if blob.updated else None
                    }

                    files.append(file_info)
            except Exception as e:
                print(f"Error listing blobs in {prefix}: {str(e)}")

            return files

        
        # List source files (CSV and JSON)
        source_files = process_blobs(source_path, 'source')
        
        # List template files (PDF)
        template_files = process_blobs(templates_path, 'templates')
        
        # Combine all files
        all_files = source_files + template_files
        
        # Sort files by name
        all_files.sort(key=lambda x: x['name'])
        source_files.sort(key=lambda x: x['name'])
        template_files.sort(key=lambda x: x['name'])
        
        # Return response
        response = {
            'files': all_files,
            'source': source_files,
            'templates': template_files
        }
        
        return (jsonify(response), 200, cors_headers)
        
    except ValueError as e:
        # Authentication/authorization errors
        return (
            jsonify({'error': str(e)}),
            401 if 'token' in str(e).lower() else 403,
            cors_headers
        )
    except Exception as e:
        # Other errors
        print(f"Error in list_campaign_files: {str(e)}")
        import traceback
        traceback.print_exc()
        return (
            jsonify({'error': f'Internal server error: {str(e)}'}),
            500,
            cors_headers
        )


# Cloud Functions 2nd gen entry point
def list_campaign_files_http(request: Request) -> tuple:
    """HTTP-triggered Cloud Function entry point."""
    return list_campaign_files(request)

