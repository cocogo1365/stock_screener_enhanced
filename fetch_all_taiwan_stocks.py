"""
擷取所有台灣股票清單
使用 FinMind API 獲取完整的上市櫃股票列表
"""
import requests
import pandas as pd
import json
from datetime import datetime
import logging

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TaiwanStockFetcher:
    """台股完整清單擷取器"""
    
    def __init__(self):
        """初始化"""
        self.base_url = 'https://api.finmindtrade.com/api/v4/data'
        
        # 嘗試載入 API token
        self.token = None
        try:
            with open('api_config.json', 'r', encoding='utf-8') as f:
                config = json.load(f)
                self.token = config.get('finmind', {}).get('api_token')
                logger.info("已載入 API token")
        except:
            logger.warning("未找到 API token，將使用無認證模式")
        
        # 設定 headers
        self.headers = {}
        if self.token:
            self.headers['Authorization'] = f'Bearer {self.token}'
    
    def fetch_all_stocks(self):
        """擷取所有台股清單"""
        logger.info("開始擷取所有台股清單...")
        
        try:
            # 使用 TaiwanStockInfo dataset 取得所有股票
            params = {
                'dataset': 'TaiwanStockInfo'
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 200 and data.get('data'):
                    all_stocks = data['data']
                    logger.info(f"API 返回 {len(all_stocks)} 筆資料")
                    
                    # 轉換為 DataFrame
                    df = pd.DataFrame(all_stocks)
                    
                    # 篩選條件
                    filtered_stocks = []
                    stats = {
                        'total': len(df),
                        'twse': 0,
                        'otc': 0,
                        'excluded_warrant': 0,
                        'excluded_etf': 0,
                        'excluded_other': 0
                    }
                    
                    for _, stock in df.iterrows():
                        stock_id = str(stock.get('stock_id', ''))
                        stock_name = stock.get('stock_name', '')
                        stock_type = stock.get('type', '')
                        industry = stock.get('industry', '')
                        
                        # 排除條件
                        exclude = False
                        
                        # 1. 只保留上市(twse)和上櫃(tpex)，以及興櫃(emerging，可選)
                        if stock_type not in ['twse', 'tpex']:  # 只要上市櫃，不含興櫃
                            stats['excluded_other'] += 1
                            continue
                        
                        # 2. 排除權證（通常是5位數或6位數代碼）
                        if len(stock_id) > 4:
                            stats['excluded_warrant'] += 1
                            continue
                        
                        # 3. 排除 ETF（00開頭的代碼，但保留一般股票）
                        if stock_id.startswith('00'):
                            # 檢查是否為 ETF（名稱通常包含 ETF 或特定關鍵字）
                            etf_keywords = ['ETF', '反', '正2', '槓桿', '期貨']
                            if any(keyword in stock_name for keyword in etf_keywords):
                                stats['excluded_etf'] += 1
                                continue
                        
                        # 4. 確保是4位數字的股票代碼
                        if not (len(stock_id) == 4 and stock_id.isdigit()):
                            stats['excluded_other'] += 1
                            continue
                        
                        # 統計市場類型
                        if stock_type == 'twse':
                            stats['twse'] += 1
                        elif stock_type == 'tpex':
                            stats['otc'] += 1
                        
                        # 加入篩選後的清單
                        filtered_stocks.append({
                            'stock_id': stock_id,
                            'stock_name': stock_name,
                            'type': stock_type,
                            'industry': industry
                        })
                    
                    # 建立最終 DataFrame
                    result_df = pd.DataFrame(filtered_stocks)
                    
                    # 排序（按股票代號）
                    result_df = result_df.sort_values('stock_id').reset_index(drop=True)
                    
                    # 顯示統計資訊
                    logger.info("="*60)
                    logger.info("股票清單統計:")
                    logger.info(f"  原始資料總數: {stats['total']}")
                    logger.info(f"  排除權證: {stats['excluded_warrant']}")
                    logger.info(f"  排除 ETF: {stats['excluded_etf']}")
                    logger.info(f"  排除其他: {stats['excluded_other']}")
                    logger.info(f"  ---")
                    logger.info(f"  篩選後總數: {len(result_df)}")
                    logger.info(f"    - 上市(TWSE): {stats['twse']}")
                    logger.info(f"    - 上櫃(OTC): {stats['otc']}")
                    logger.info("="*60)
                    
                    return result_df
                else:
                    logger.error(f"API 返回錯誤: {data.get('msg', 'Unknown error')}")
                    return None
            else:
                logger.error(f"HTTP 錯誤: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"擷取股票清單失敗: {e}")
            return None
    
    def save_to_csv(self, df, filename='all_taiwan_stocks.csv'):
        """儲存到 CSV 檔案"""
        if df is not None and not df.empty:
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            logger.info(f"已儲存 {len(df)} 檔股票到 {filename}")
            return True
        return False
    
    def get_sample_data(self, df, sample_size=10):
        """顯示範例資料"""
        if df is not None and not df.empty:
            print("\n範例股票資料:")
            print("-"*80)
            # 顯示前幾筆
            print(df.head(sample_size).to_string())
            print("-"*80)
            # 顯示產業分布
            if 'industry' in df.columns:
                print("\n產業分布 (前10):")
                industry_counts = df['industry'].value_counts().head(10)
                for industry, count in industry_counts.items():
                    print(f"  {industry}: {count} 檔")

def main():
    """主程式"""
    print("="*80)
    print("台灣股票完整清單擷取程式")
    print("="*80)
    
    fetcher = TaiwanStockFetcher()
    
    # 擷取所有股票
    all_stocks_df = fetcher.fetch_all_stocks()
    
    if all_stocks_df is not None:
        # 儲存完整清單
        fetcher.save_to_csv(all_stocks_df, 'all_taiwan_stocks.csv')
        
        # 更新 real_stock_list.csv（供現有程式使用）
        fetcher.save_to_csv(all_stocks_df, 'real_stock_list.csv')
        
        # 顯示範例資料
        fetcher.get_sample_data(all_stocks_df, sample_size=20)
        
        print(f"\n成功擷取 {len(all_stocks_df)} 檔台灣股票！")
        print("檔案已儲存為:")
        print("  1. all_taiwan_stocks.csv (完整清單)")
        print("  2. real_stock_list.csv (更新現有清單)")
        
        return all_stocks_df
    else:
        print("\n擷取失敗，請檢查網路連線或 API 設定")
        return None

if __name__ == "__main__":
    main()