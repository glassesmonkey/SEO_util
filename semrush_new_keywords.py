#找到六个月内才有趋势的新词

import pandas as pd
import numpy as np

def analyze_trends(file_path):
    # 读取Excel文件
    df = pd.read_excel(file_path)
    
    # 将trend列分割成12个月的数据
    df[['month_' + str(i+1) for i in range(12)]] = df['Trend'].str.split(',', expand=True).astype(float)
    
    # 定义函数来判断是否为"新词"
    def is_new_keyword(row):
        first_half = row[['month_' + str(i+1) for i in range(6)]]
        second_half = row[['month_' + str(i+1) for i in range(6, 12)]]
        return (first_half.sum() == 0) and (second_half.sum() > 0)
    
    # 添加新列"is_new_keyword"
    df['is_new_keyword'] = df.apply(is_new_keyword, axis=1)
    
    # 计算后6个月的平均搜索量
    df['avg_second_half'] = df[['month_' + str(i+1) for i in range(6, 12)]].mean(axis=1)
    
    # 对新词按后6个月平均搜索量排序
    new_keywords = df[df['is_new_keyword']].sort_values('avg_second_half', ascending=False)
    
    # 保存结果
    df.to_excel('analyzed_keywords.xlsx', index=False)
    new_keywords.to_excel('new_keywords.xlsx', index=False)
    
    return df, new_keywords

# 使用函数
file_path = 'Generator_broad-match_us_2024-09-16.xlsx'  # 替换为您的Excel文件路径
all_data, new_words = analyze_trends(file_path)

print(f"总关键词数: {len(all_data)}")
print(f"新词数量: {len(new_words)}")
print("\n前10个新词:")
print(new_words[['Keyword', 'avg_second_half']].head(10))  # 假设有一列名为'keyword'
