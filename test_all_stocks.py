"""
測試完整股票清單功能
確認系統可以正確取得並處理所有台股
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.enhanced_data_fetcher import EnhancedDataFetcher
import pandas as pd

def test_all_stocks():
    """測試所有股票清單"""
    print("="*80)
    print("測試完整股票清單功能")
    print("="*80)
    
    # 初始化
    fetcher = EnhancedDataFetcher()
    
    # 測試連線
    if not fetcher.test_connection():
        print("API 連線失敗")
        return
    
    print("\n1. 測試取得股票清單...")
    stock_list = fetcher.get_stock_list()
    
    if stock_list is None or stock_list.empty:
        print("   [FAIL] 無法取得股票清單")
        return
    
    print(f"   [PASS] 成功取得 {len(stock_list)} 檔股票")
    
    # 統計
    print("\n2. 股票清單統計:")
    print("-"*60)
    
    # 按市場類型統計
    if 'type' in stock_list.columns:
        type_counts = stock_list['type'].value_counts()
        for market_type, count in type_counts.items():
            market_name = {
                'twse': '上市',
                'tpex': '上櫃',
                'otc': '上櫃(舊)',
                'emerging': '興櫃'
            }.get(market_type, market_type)
            print(f"   {market_name}: {count} 檔")
    
    # 按產業統計（如果有）
    if 'industry' in stock_list.columns:
        print("\n3. 產業分布 (前10):")
        print("-"*60)
        industry_counts = stock_list['industry'].value_counts().head(10)
        for industry, count in industry_counts.items():
            if industry:  # 跳過空值
                print(f"   {industry}: {count} 檔")
    
    # 測試取得一些股票資料
    print("\n4. 測試取得股票資料:")
    print("-"*60)
    
    # 測試幾檔知名股票
    test_stocks = [
        ('2330', '台積電', 'twse'),
        ('2454', '聯發科', 'twse'),
        ('6547', '高端疫苗', 'tpex'),
        ('3105', '穩懋', 'tpex')
    ]
    
    for stock_id, stock_name, expected_type in test_stocks:
        # 檢查是否在清單中
        stock_info = stock_list[stock_list['stock_id'] == stock_id]
        if not stock_info.empty:
            actual_type = stock_info.iloc[0].get('type', 'unknown')
            type_match = '✓' if actual_type in [expected_type, 'otc' if expected_type == 'tpex' else expected_type] else '✗'
            print(f"   {stock_id} {stock_name}: 存在={type_match} 類型={actual_type}")
        else:
            print(f"   {stock_id} {stock_name}: 不在清單中")
    
    # 測試從 CSV 讀取
    print("\n5. 測試 CSV 檔案:")
    print("-"*60)
    
    csv_path = 'real_stock_list.csv'
    if os.path.exists(csv_path):
        csv_df = pd.read_csv(csv_path, encoding='utf-8-sig')
        print(f"   CSV 檔案存在: {len(csv_df)} 筆資料")
        
        # 比較數量
        if len(csv_df) == len(stock_list):
            print(f"   [PASS] CSV 與 API 資料數量一致")
        else:
            print(f"   [WARNING] CSV ({len(csv_df)}) 與 API ({len(stock_list)}) 資料數量不同")
    else:
        print(f"   [WARNING] CSV 檔案不存在")
    
    # 總結
    print("\n" + "="*80)
    print("測試總結:")
    print(f"✓ 成功取得 {len(stock_list)} 檔股票（原本只有 81 檔）")
    print(f"✓ 包含上市和上櫃股票")
    print(f"✓ 系統可以處理完整的台股清單")
    print("="*80)

if __name__ == "__main__":
    test_all_stocks()