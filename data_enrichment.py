import pandas as pd

def process_mortgage_data_final():
    print("Starting process... / 开始处理...")

    # 1. Load MORTGAGE30US.csv
    try:
        mortgage_df = pd.read_csv("MORTGAGE30US.csv")
        
        # 使用你提供的列名：observation_date
        date_col = 'observation_date'
        
        if date_col not in mortgage_df.columns:
            print(f"Error: Could not find '{date_col}' in columns: {list(mortgage_df.columns)}")
            return
        
        # 转换日期格式
        mortgage_df[date_col] = pd.to_datetime(mortgage_df[date_col])
        mortgage_df.set_index(date_col, inplace=True)
        
        print(f"Successfully loaded mortgage data using column: {date_col}")
    except Exception as e:
        print(f"Error loading MORTGAGE30US.csv: {e}")
        return

    # 2. Resample to monthly averages (计算月度平均值)
    monthly_mortgage = mortgage_df.resample('MS').mean().reset_index()
    # 生成用于匹配的 key
    monthly_mortgage['year_month'] = monthly_mortgage[date_col].dt.to_period('M').astype(str)
    # 只保留合并需要的列
    monthly_mortgage = monthly_mortgage[['year_month', 'MORTGAGE30US']]

    # 3. Load and Merge onto cleaned datasets
    files_to_process = ["sold_combined.csv", "listings_combined.csv"]
    
    for file_name in files_to_process:
        try:
            df = pd.read_csv(file_name)
            
            # 自动生成 year_month (如果不存在)
            if 'year_month' not in df.columns:
                # 寻找房产数据中的日期列 (通常叫 list_date, sold_date 或 date)
                potential_date_col = next((c for c in df.columns if 'date' in c.lower()), None)
                if potential_date_col:
                    df['year_month'] = pd.to_datetime(df[potential_date_col]).dt.to_period('M').astype(str)
                    print(f"Created 'year_month' from '{potential_date_col}' in {file_name}")
                else:
                    print(f"Error: No date column found in {file_name} to create a merge key.")
                    continue

            # 执行合并 (Merge)
            enriched_df = pd.merge(df, monthly_mortgage, on='year_month', how='left')

            # 4. Validation Check (验证是否存在空值)
            null_count = enriched_df['MORTGAGE30US'].isnull().sum()
            if null_count > 0:
                print(f"⚠️ {file_name}: Validation Failed! {null_count} rows are missing rates.")
                # 打印出缺失的月份以便排查
                missing_months = enriched_df[enriched_df['MORTGAGE30US'].isnull()]['year_month'].unique()
                print(f"   Missing months: {missing_months}")
            else:
                print(f"✅ {file_name}: Validation passed (no nulls).")

            # 保存结果
            output_name = f"enriched_{file_name}"
            enriched_df.to_csv(output_name, index=False)
            print(f"Saved: {output_name}")

        except FileNotFoundError:
            print(f"Skip: {file_name} not found in current directory.")

    print("\nProcess Complete. / 处理完毕。")

if __name__ == "__main__":
    process_mortgage_data_final()