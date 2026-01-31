#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch all GmbH companies from Handelsregister by postcode using bundesAPI/handelsregister library.

Installation:
    pip install git+https://github.com/bundesAPI/handelsregister.git
    # OR if available via PyPI:
    # pip install handelsregister

Usage:
    python fetch_gmbh_by_postcode.py <postcode> [output.csv]

Example:
    python fetch_gmbh_by_postcode.py 80331 gmbh_80331.csv
"""

import csv
import sys
from typing import List, Dict, Optional

try:
    # Try importing the bundesAPI handelsregister library
    # The library structure may vary, so we'll try different import patterns
    try:
        from handelsregister.handelsregister import Handelsregister
    except ImportError:
        try:
            from handelsregister import Handelsregister
        except ImportError:
            # If it's a module with functions rather than a class
            import handelsregister
            Handelsregister = None
except ImportError:
    print("Error: handelsregister library not found.")
    print("Please install it with:")
    print("  pip install git+https://github.com/bundesAPI/handelsregister.git")
    print("\nOr clone the repository and install:")
    print("  git clone https://github.com/bundesAPI/handelsregister.git")
    print("  cd handelsregister")
    print("  pip install -e .")
    sys.exit(1)


def fetch_gmbh_by_postcode(postcode: str, output_file: Optional[str] = None) -> List[Dict]:
    """
    Fetch all GmbH companies from a specific postcode.
    
    Args:
        postcode: 5-digit German postcode (e.g., "80331")
        output_file: Optional CSV file path to save results
        
    Returns:
        List of company dictionaries
    """
    # Prepare search parameters based on bundesAPI/handelsregister documentation
    # rechtsform=8 means "Gesellschaft mit beschränkter Haftung" (GmbH)
    # suchTyp="e" means extended search
    search_params = {
        "postleitzahl": postcode,
        "rechtsform": "8",  # GmbH
        "suchTyp": "e",  # extended search
        "ergebnisseProSeite": "100",  # max results per page
        "btnSuche": "Suchen"
    }
    
    print(f"Searching for GmbH companies in postcode {postcode}...")
    
    all_companies = []
    
    try:
        # Try different ways to use the library
        if Handelsregister:
            # If it's a class, instantiate it
            client = Handelsregister()
            
            # Try different method names
            if hasattr(client, 'search'):
                results = client.search(**search_params)
            elif hasattr(client, 'query'):
                results = client.query(**search_params)
            elif hasattr(client, 'fetch'):
                results = client.fetch(**search_params)
            else:
                # Try calling it directly with params
                results = client(**search_params)
        else:
            # If it's a module with functions
            if hasattr(handelsregister, 'search'):
                results = handelsregister.search(**search_params)
            elif hasattr(handelsregister, 'query'):
                results = handelsregister.query(**search_params)
            else:
                raise AttributeError("Could not find search/query method in handelsregister module")
        
        # Process results - handle different return types
        if results:
            # If results is a list
            if isinstance(results, list):
                for company in results:
                    company_data = _extract_company_data(company, postcode)
                    if company_data:
                        all_companies.append(company_data)
            # If results is a dict with a 'results' or 'companies' key
            elif isinstance(results, dict):
                companies_list = results.get('results') or results.get('companies') or results.get('data', [])
                for company in companies_list:
                    company_data = _extract_company_data(company, postcode)
                    if company_data:
                        all_companies.append(company_data)
            # If results is iterable but not a list
            elif hasattr(results, '__iter__'):
                for company in results:
                    company_data = _extract_company_data(company, postcode)
                    if company_data:
                        all_companies.append(company_data)
            else:
                print(f"Warning: Unexpected result format. Type: {type(results)}")
                print(f"Results preview: {str(results)[:200]}")
    
    except Exception as e:
        print(f"Error during search: {e}")
        import traceback
        traceback.print_exc()
        return []
    
    print(f"Found {len(all_companies)} GmbH companies")
    
    # Save to CSV if output file specified
    if output_file and all_companies:
        fieldnames = ["rechtsform", "firmenname", "ort", "plz", "strasse_hausnr", "registernummer"]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_companies)
        
        print(f"Results saved to {output_file}")
    
    return all_companies


def _extract_company_data(company, postcode: str) -> Optional[Dict]:
    """
    Extract company data from various possible formats.
    
    Args:
        company: Company object/dict from the API
        postcode: Fallback postcode if not in company data
        
    Returns:
        Dictionary with company data or None if invalid
    """
    try:
        # Handle dict-like objects
        if isinstance(company, dict):
            name = company.get('name') or company.get('firmenname') or company.get('firma', '')
            ort = company.get('ort') or company.get('city') or company.get('sitz', '')
            plz = company.get('plz') or company.get('postcode') or company.get('postleitzahl', postcode)
            strasse = company.get('strasse') or company.get('street') or company.get('anschrift', '')
            reg_num = company.get('registernummer') or company.get('register_number') or company.get('register', '')
        else:
            # Handle object-like structures
            name = getattr(company, 'name', None) or getattr(company, 'firmenname', None) or getattr(company, 'firma', '')
            ort = getattr(company, 'ort', None) or getattr(company, 'city', None) or getattr(company, 'sitz', '')
            plz = getattr(company, 'plz', None) or getattr(company, 'postcode', None) or getattr(company, 'postleitzahl', postcode)
            strasse = getattr(company, 'strasse', None) or getattr(company, 'street', None) or getattr(company, 'anschrift', '')
            reg_num = getattr(company, 'registernummer', None) or getattr(company, 'register_number', None) or getattr(company, 'register', '')
        
        # Convert to strings and clean
        name = str(name).strip() if name else ''
        if not name:
            return None  # Skip companies without names
        
        return {
            "rechtsform": "GmbH",
            "firmenname": name,
            "ort": str(ort).strip() if ort else '',
            "plz": str(plz).strip() if plz else postcode,
            "strasse_hausnr": str(strasse).strip() if strasse else '',
            "registernummer": str(reg_num).strip() if reg_num else ''
        }
    except Exception as e:
        print(f"Warning: Error extracting company data: {e}")
        return None


def main():
    if len(sys.argv) < 2:
        print("Usage: python fetch_gmbh_by_postcode.py <postcode> [output.csv]")
        print("\nExample:")
        print("  python fetch_gmbh_by_postcode.py 80331")
        print("  python fetch_gmbh_by_postcode.py 80331 gmbh_80331.csv")
        sys.exit(1)
    
    postcode = sys.argv[1].strip()
    output_file = sys.argv[2].strip() if len(sys.argv) > 2 else None
    
    # Validate postcode format (5 digits)
    if not postcode.isdigit() or len(postcode) != 5:
        print(f"Error: Postcode must be 5 digits. Got: {postcode}")
        sys.exit(1)
    
    # Fetch companies
    companies = fetch_gmbh_by_postcode(postcode, output_file)
    
    # Print summary
    if companies:
        print(f"\nSummary: Found {len(companies)} GmbH companies in postcode {postcode}")
        print("\nFirst few results:")
        for i, company in enumerate(companies[:5], 1):
            print(f"\n{i}. {company['firmenname']}")
            print(f"   Address: {company['strasse_hausnr']}, {company['plz']} {company['ort']}")
            print(f"   Register: {company['registernummer']}")
    else:
        print(f"No GmbH companies found in postcode {postcode}")


if __name__ == "__main__":
    main()

