# %%
import pandas as pd
import numpy as np

# %%
def clean_real_estate_data(file_path, label):
    """
    Weeks 4-5: Data Cleaning and Preparation
    """
    print(f"--- Starting cleaning for: {label} ---")
    
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
        return None

    initial_row_count = len(df)

# 0. Cleanup
    
    # A. Remove Duplicate Columns from Merges 
    df = df.loc[:, ~df.columns.str.contains('\.1$')]

    # B. Remove Completely Empty Columns
    df = df.dropna(axis=1, how='all')

    # C. Remove Highly Missing Columns (> 90% NA)
    missing_threshold = 0.90
    df = df.loc[:, df.isnull().mean() < missing_threshold]

    # D. Remove Manually Identified Redundant Metadata
    manual_drop = ['InternalID', 'Unnamed: 0', 'UnparsedAddress']
    df = df.drop(columns=[c for c in manual_drop if c in df.columns], errors='ignore')

# 1. Date Conversion 
    date_fields = ['CloseDate', 'PurchaseContractDate', 'ListingContractDate', 'ContractStatusChangeDate']
    for col in date_fields:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

# 2. Date Consistency Flags 
    df['listing_after_close_flag'] = False
    df['purchase_after_close_flag'] = False
    df['negative_timeline_flag'] = False

    if 'ListingContractDate' in df.columns and 'CloseDate' in df.columns:
        df['listing_after_close_flag'] = df['ListingContractDate'] > df['CloseDate']
    
    if 'PurchaseContractDate' in df.columns and 'CloseDate' in df.columns:
        df['purchase_after_close_flag'] = df['PurchaseContractDate'] > df['CloseDate']
    
    if 'ListingContractDate' in df.columns and 'PurchaseContractDate' in df.columns:
        df['negative_timeline_flag'] = df['PurchaseContractDate'] < df['ListingContractDate']

# 3. Numeric Validation 
    # Handle ClosePrice, LivingArea, DOM, Bed, and Bath
    invalid_mask = (
        (df.get('ClosePrice', 1) <= 0) | 
        (df.get('LivingArea', 1) <= 0) | 
        (df.get('DaysOnMarket', 0) < 0)
    )
    
# 4. Geographic Data Checks 
    df['geo_missing_flag'] = df['Latitude'].isna() | df['Longitude'].isna()
    df['geo_zero_flag'] = (df['Latitude'] == 0) | (df['Longitude'] == 0)
    df['geo_invalid_lon_flag'] = df['Longitude'] > 0  # California Longitude must be negative

# 5. Save Cleaned Dataset 
    df_cleaned = df[~invalid_mask].copy()
    output_filename = f"{label.lower()}_analysis_ready.csv"
    df_cleaned.to_csv(output_filename, index=False)
    
    print(f"Task Complete. Before: {initial_row_count} rows, After: {len(df_cleaned)} rows.")
    print(f"Output saved to: {output_filename}\n")
    return df_cleaned


# %%
# Main execution
if __name__ == "__main__":
    # Process Listings and Sold separately 
    clean_real_estate_data('enriched_listings_combined.csv', 'Listings')
    clean_real_estate_data('enriched_sold_combined.csv', 'Sold')


