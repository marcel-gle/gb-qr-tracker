import firebase_admin
from firebase_admin import credentials, firestore

def count_links_for_multiple_campaigns(service_account_path: str, campaign_ids: list[str]) -> None:
    """
    Count all 'links' documents for multiple campaign references.

    Args:
        service_account_path (str): Path to the Firebase service account JSON file.
        campaign_ids (list[str]): List of campaign document IDs (from 'campaigns' collection).

    Prints:
        The count per campaign and the total count.
    """
    # Initialize Firebase app if not already initialized
    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred)

    db = firestore.client()

    total_count = 0

    for campaign_id in campaign_ids:
        campaign_ref = db.collection('campaigns').document(campaign_id)
        links_query = db.collection('links').where('campaign_ref', '==', campaign_ref)
        results = list(links_query.stream())

        count = len(results)
        total_count += count

        print(f"Campaign {campaign_id}: {count} links")

    print(f"\nTotal links across all campaigns: {total_count}")


# Example usage:
if __name__ == "__main__":
    #service_account_path = "/Users/marcelgleich/Desktop/Software/Firebase_Service//gb-qr-tracker-firebase-adminsdk-fbsvc-e89462f043.json" #prod
    service_account_path = "/Users/marcelgleich/Desktop/Software/Firebase_Service/gb-qr-tracker-dev-firebase-adminsdk-fbsvc-51be21988f.json" #dev
    campaign_ids = ["395b3070-5f35-485f-bf01-a734e041e37b", "7d106cd6-6953-47c2-b50e-03ae3afb12eb", "JowSxxqUb2Slc4wXsspT"]
    count_links_for_multiple_campaigns(service_account_path, campaign_ids)
