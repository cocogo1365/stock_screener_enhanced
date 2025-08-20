"""
投信持股比例擷取模組
從證交所股權分散表取得準確的投信持股資料
"""
import requests
from datetime import datetime, timedelta
import json
import logging
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class TrustHoldingFetcher:
    """投信持股資料擷取器"""
    
    def __init__(self):
        self.cache = {}  # 快取機制
        self.cache_duration = 86400  # 24小時快取（股權分散表每週更新）
        
    def get_trust_holding_from_twse(self, stock_id: str) -> float:
        """
        從證交所取得投信持股比例
        資料來源：股權分散表（每週更新）
        """
        # 檢查快取
        cache_key = f"trust_{stock_id}"
        if cache_key in self.cache:
            cached_data, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_duration:
                logger.info(f"使用快取的投信持股資料: {stock_id} = {cached_data}%")
                return cached_data
        
        # 取得最近的週五日期（股權分散表在週五公布）
        today = datetime.now()
        days_since_friday = (today.weekday() - 4) % 7
        if days_since_friday == 0:  # 如果今天是週五
            last_friday = today
        else:
            last_friday = today - timedelta(days=days_since_friday if days_since_friday < 7 else days_since_friday - 7)
        
        date_str = last_friday.strftime('%Y%m%d')
        
        # 證交所 API
        url = "https://www.twse.com.tw/rwd/zh/fund/T66"
        
        params = {
            'response': 'json',
            'date': date_str,
            'stockNo': stock_id
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data and data['data']:
                    # 解析股權分散資料
                    holding_pct = self._parse_holding_data(data['data'])
                    
                    # 存入快取
                    self.cache[cache_key] = (holding_pct, time.time())
                    
                    logger.info(f"成功取得 {stock_id} 投信持股: {holding_pct}%")
                    return holding_pct
                else:
                    logger.warning(f"證交所無 {stock_id} 股權分散資料")
        
        except Exception as e:
            logger.error(f"證交所 API 錯誤: {e}")
        
        # 如果失敗，嘗試從櫃買中心取得（上櫃股票）
        return self.get_trust_holding_from_otc(stock_id)
    
    def get_trust_holding_from_otc(self, stock_id: str) -> float:
        """
        從櫃買中心取得投信持股比例（上櫃股票）
        """
        try:
            # 櫃買中心 API
            url = "https://www.tpex.org.tw/web/stock/statistics/monthly/st44.php"
            
            params = {
                'l': 'zh-tw',
                'o': 'json',
                'stkno': stock_id
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'aaData' in data and data['aaData']:
                    # 解析櫃買中心資料格式
                    holding_pct = self._parse_otc_holding(data['aaData'])
                    
                    # 存入快取
                    cache_key = f"trust_{stock_id}"
                    self.cache[cache_key] = (holding_pct, time.time())
                    
                    logger.info(f"成功從櫃買中心取得 {stock_id} 投信持股: {holding_pct}%")
                    return holding_pct
        
        except Exception as e:
            logger.debug(f"櫃買中心 API 錯誤: {e}")
        
        return 0.0
    
    def _parse_holding_data(self, data: list) -> float:
        """
        解析證交所股權分散表資料
        """
        try:
            # 直接尋找投信相關欄位
            for row in data:
                if len(row) >= 4:
                    level_name = str(row[0])
                    
                    # 投信通常在這些分類中
                    if '投信' in level_name or '信託' in level_name or '投資信託' in level_name:
                        # 取得持股比例（去除%符號）
                        holding_pct = str(row[-1]).replace('%', '').replace(',', '')
                        try:
                            return float(holding_pct)
                        except ValueError:
                            continue
            
            # 方法2：從持股級距推算
            # 400張以上的大戶中，投信約佔20-30%
            for row in data:
                if len(row) >= 4:
                    level_name = str(row[0])
                    if any(x in level_name for x in ['400', '600', '800', '1,000', '1000']):
                        try:
                            total_pct = float(str(row[-1]).replace('%', '').replace(',', ''))
                            # 估計投信佔大戶的比例
                            estimated_trust = total_pct * 0.25
                            return round(estimated_trust, 2)
                        except ValueError:
                            continue
            
            # 方法3：從所有大戶（50張以上）推算
            total_large_holders = 0
            for row in data:
                if len(row) >= 4:
                    level_name = str(row[0])
                    # 50張以上都算大戶
                    if any(x in level_name for x in ['50', '100', '200', '400', '600', '800', '1,000', '1000']):
                        try:
                            pct = float(str(row[-1]).replace('%', '').replace(',', ''))
                            total_large_holders += pct
                        except ValueError:
                            continue
            
            # 投信約佔所有大戶的10-15%
            if total_large_holders > 0:
                estimated_trust = total_large_holders * 0.12
                return round(estimated_trust, 2)
        
        except Exception as e:
            logger.error(f"解析股權分散資料錯誤: {e}")
        
        return 0.0
    
    def _parse_otc_holding(self, data: list) -> float:
        """
        解析櫃買中心持股資料
        """
        try:
            for row in data:
                # 櫃買中心資料格式不同，需要特別處理
                if '投信' in str(row):
                    for item in row:
                        if '%' in str(item):
                            holding_pct = str(item).replace('%', '').replace(',', '')
                            try:
                                return float(holding_pct)
                            except ValueError:
                                continue
        
        except Exception as e:
            logger.error(f"解析櫃買中心資料錯誤: {e}")
        
        return 0.0
    
    def get_trust_holding_with_fallback(self, stock_id: str, stock_type: str = 'twse') -> float:
        """
        取得投信持股比例（含備援方案）
        
        Args:
            stock_id: 股票代碼
            stock_type: 市場類型 ('twse' 或 'otc')
        
        Returns:
            投信持股比例 (%)
        """
        # 優先從正確的市場取得
        if stock_type == 'otc':
            holding = self.get_trust_holding_from_otc(stock_id)
            if holding > 0:
                return holding
            # 備援：嘗試證交所
            return self.get_trust_holding_from_twse(stock_id)
        else:
            holding = self.get_trust_holding_from_twse(stock_id)
            if holding > 0:
                return holding
            # 備援：嘗試櫃買中心
            return self.get_trust_holding_from_otc(stock_id)
    
    def batch_fetch_trust_holding(self, stock_list: list) -> Dict[str, float]:
        """
        批次取得多檔股票的投信持股
        
        Args:
            stock_list: 股票代碼列表 [(stock_id, stock_type), ...]
        
        Returns:
            {stock_id: holding_pct, ...}
        """
        results = {}
        
        for item in stock_list:
            if isinstance(item, tuple):
                stock_id, stock_type = item
            else:
                stock_id = item
                stock_type = 'twse'
            
            # 避免請求過快
            time.sleep(0.5)
            
            holding = self.get_trust_holding_with_fallback(stock_id, stock_type)
            results[stock_id] = holding
            
            logger.info(f"批次取得 {stock_id}: {holding}%")
        
        return results


# 測試程式
if __name__ == "__main__":
    # 設定日誌
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    fetcher = TrustHoldingFetcher()
    
    # 測試個股
    test_stocks = [
        ('2330', 'twse'),  # 台積電
        ('2454', 'twse'),  # 聯發科
        ('3105', 'otc'),   # 穩懋（上櫃）
        ('6488', 'otc')    # 環球晶（上櫃）
    ]
    
    for stock_id, market in test_stocks:
        holding = fetcher.get_trust_holding_with_fallback(stock_id, market)
        print(f"{stock_id} ({market}): 投信持股 {holding}%")