import pandas as pd

def clean_data(file_path, name):
    print(f"--- Cleaning & Summary Report: {name} ---")
    df = pd.read_csv(file_path)
    
    # (1) Unique property types
    unique_types = df['PropertyType'].unique()
    print(f"Unique Property Types found: {unique_types}")

    '''
    Unique Property Types found: <StringArray>
['Residential', 'ResidentialLease', 'ResidentialIncome']
    '''
    
    # (2)Null-count summary table
    null_summary = df.isna().sum()
    print("\nNull-count summary (first 10 columns):")
    print(null_summary.head(10))
    
    # (3) Missingness report & filtering
    threshold = 0.9 * len(df)
    cols_to_drop = df.columns[df.isna().sum() > threshold]
    print(f"\nColumns flagged for removal (>90% N/A): {list(cols_to_drop)}")
    df_filtered = df.drop(columns=cols_to_drop)
    
    # (4) Numeric distribution summary
    # 目标列：ClosePrice, LivingArea, DaysOnMarket
    targets = ["ClosePrice", "LivingArea", "DaysOnMarket"]
    # 检查列是否存在（Sold 有 ClosePrice，Listings 可能没有）
    existing_targets = [c for c in targets if c in df_filtered.columns]
    
    if existing_targets:
        print(f"\nDistribution summary for {existing_targets}:")
        # .describe() 包含 min, max, mean, median(50%), percentiles
        print(df_filtered[existing_targets].describe())

    '''
Distribution summary for ['ClosePrice', 'LivingArea', 'DaysOnMarket']:
         ClosePrice    LivingArea   DaysOnMarket
count  5.620660e+05  5.478900e+05  562073.000000
mean   8.523756e+05  3.515127e+03      38.739119
std    1.316066e+06  1.228389e+06      53.587676
min    0.000000e+00  0.000000e+00    -288.000000
25%    5.500000e+04  1.187000e+03       8.000000
50%    6.550000e+05  1.594000e+03      21.000000
75%    1.088000e+06  2.170000e+03      50.000000
max    4.120000e+08  9.090909e+08   12430.000000
'''
    
    # Save filtered dataset
    output_name = f"{name.lower()}_filtered.csv"
    df_filtered.to_csv(output_name, index=False)
    print(f"Saved filtered dataset to {output_name}\n")

if __name__ == "__main__":
    clean_data('listings_combined.csv', 'Listings')
    clean_data('sold_combined.csv', 'Sold')

