import pandas as pd
import glob
import os

# 1. 更加通用的搜索模式
listing_files = glob.glob('**/*Listing*.csv', recursive=True)
sold_files = glob.glob('**/*Sold*.csv', recursive=True)

def combine_and_filter(file_list, name):
    print(f"\n--- Starting {name} Process ---")
    
    # 调试：打印找到的文件列表，看看是不是空的
    if not file_list:
        print(f"❌ 未找到任何文件！请检查：")
        print(f"   - 当前运行目录是否是 'idx exchange'？")
        print(f"   - 文件名里是否确实包含 '{name[:-1]}' 这个单词？")
        return None
    
    print(f"✅ 找到 {len(file_list)} 个 {name} 相关文件.")
    
    df_list = []
    total_initial_rows = 0
    
    for f in file_list:
        try:
            # 使用 low_memory=False 防止大文件警告
            temp_df = pd.read_csv(f, low_memory=False)
            total_initial_rows += len(temp_df)
            df_list.append(temp_df)
            print(f"   读取成功: {os.path.basename(f)} ({len(temp_df)} 行)")
        except Exception as e:
            print(f"   ⚠️ 读取失败 {f}: {e}")
    
    # 合并
    combined_df = pd.concat(df_list, ignore_index=True)
    
    # 过滤住宅类型
    # 注意：如果你的 CSV 里 PropertyType 的值全是大写（RESIDENTIAL），请把下面改成 'RESIDENTIAL'
    before_filter = len(combined_df)
    combined_df = combined_df[combined_df['PropertyType'].str.contains('Residential', case=False, na=False)]
    after_filter = len(combined_df)
    
    print(f"📊 Combined Statistics:")
    print(f"   - Total original rows: {total_initial_rows}")
    print(f"   - Rows after filtering 'Residential': {after_filter}")
    print(f"   - Rows after excluding non-residential: {before_filter - after_filter}")
    
    # 保存
    output_file = f"{name.lower()}_combined.csv"
    combined_df.to_csv(output_file, index=False)
    print(f"💾 已保存至: {output_file}")
    return combined_df

if __name__ == "__main__":
    # 检查当前工作目录
    print(f"当前 Python 运行目录: {os.getcwd()}")
    
    combine_and_filter(listing_files, "Listings")
    combine_and_filter(sold_files, "Sold")

#SOLD
'''
Combined Statistics:
   - Total original rows: 606533
   - Rows after filtering 'Residential': 562073
'''
#List
'''Combined Statistics:
   - Total original rows: 675542
   - Rows after filtering 'Residential': 611388
   '''