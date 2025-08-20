"""
ROE 計算模組
根據 FinMind 財報資料手動計算 ROE
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class ROECalculator:
    """ROE 計算器"""
    
    def calculate_roe_from_financial(self, financial_df: pd.DataFrame) -> float:
        """
        從財報資料計算 ROE
        
        Args:
            financial_df: FinMind 財報資料 DataFrame
        
        Returns:
            ROE 百分比
        """
        if financial_df is None or financial_df.empty:
            logger.warning("財報資料為空")
            return 0.0
        
        # 可能的淨利欄位名稱
        net_income_keywords = [
            '本期淨利（淨損）',
            '本期稅後淨利',
            '稅後淨利',
            '淨利',
            '稅後純益',
            'NetIncomeAfterTax',
            'NetIncome',
            'Profit'
        ]
        
        # 可能的股東權益欄位名稱
        equity_keywords = [
            '權益總計',
            '權益總額',
            '股東權益總計',
            '歸屬於母公司業主之權益',
            '股東權益合計',
            'TotalEquity',
            'Equity',
            'ShareholderEquity'
        ]
        
        net_income = None
        equity = None
        
        # 調試：列出所有 type 值
        if 'type' in financial_df.columns:
            unique_types = financial_df['type'].unique()
            logger.debug(f"財報中的 type 值: {unique_types[:20]}")  # 只顯示前20個
        
        # 尋找淨利
        for ni_keyword in net_income_keywords:
            if 'type' in financial_df.columns:
                ni_data = financial_df[financial_df['type'].str.contains(ni_keyword, na=False, case=False)]
                if not ni_data.empty:
                    # 取最新一期的值
                    net_income = ni_data['value'].iloc[-1] if 'value' in ni_data.columns else None
                    if net_income is not None:
                        logger.info(f"找到淨利項目: {ni_keyword} = {net_income}")
                        break
        
        # 尋找股東權益
        for eq_keyword in equity_keywords:
            if 'type' in financial_df.columns:
                eq_data = financial_df[financial_df['type'].str.contains(eq_keyword, na=False, case=False)]
                if not eq_data.empty:
                    # 取最新一期的值
                    equity = eq_data['value'].iloc[-1] if 'value' in eq_data.columns else None
                    if equity is not None:
                        logger.info(f"找到權益項目: {eq_keyword} = {equity}")
                        break
        
        # 計算 ROE
        if net_income is not None and equity is not None and equity > 0:
            # 判斷是否需要年化（季報需要 *4）
            if 'date' in financial_df.columns:
                latest_date = pd.to_datetime(financial_df['date'].max())
                # 判斷是否為季報（通常每季公布）
                if latest_date.month in [3, 6, 9, 12]:
                    quarter = (latest_date.month - 1) // 3 + 1
                    annualization_factor = 4 / quarter  # 年化因子
                else:
                    annualization_factor = 1
            else:
                annualization_factor = 4  # 預設為季報
            
            roe = (net_income / equity) * 100 * annualization_factor
            logger.info(f"計算 ROE: 淨利={net_income}, 權益={equity}, 年化因子={annualization_factor}, ROE={roe:.2f}%")
            return round(roe, 2)
        
        logger.warning(f"無法計算 ROE: 淨利={net_income}, 權益={equity}")
        return 0.0
    
    def get_roe_with_fallback(self, stock_id: str, financial_df: pd.DataFrame = None) -> float:
        """
        取得 ROE，包含多重備案
        
        Args:
            stock_id: 股票代碼
            financial_df: 財報資料（可選）
        
        Returns:
            ROE 百分比
        """
        # 方法1: 從財報計算
        if financial_df is not None:
            roe = self.calculate_roe_from_financial(financial_df)
            if roe > 0:
                return roe
        
        # 方法2: 使用產業平均值作為備案
        industry_avg_roe = self.get_industry_average_roe(stock_id)
        if industry_avg_roe > 0:
            logger.info(f"使用產業平均 ROE: {industry_avg_roe}%")
            return industry_avg_roe
        
        # 方法3: 使用台股整體平均值
        logger.warning(f"{stock_id} 使用台股平均 ROE: 15%")
        return 15.0
    
    def get_industry_average_roe(self, stock_id: str) -> float:
        """
        取得產業平均 ROE
        
        Args:
            stock_id: 股票代碼
        
        Returns:
            產業平均 ROE
        """
        # 簡化的產業分類與平均 ROE
        industry_roe = {
            # 半導體業
            '2330': 25.0,  # 台積電
            '2454': 20.0,  # 聯發科
            '2303': 12.0,  # 聯電
            
            # 電子零組件
            '2317': 15.0,  # 鴻海
            '2308': 18.0,  # 台達電
            '2382': 16.0,  # 廣達
            
            # 金融業
            '2881': 12.0,  # 富邦金
            '2882': 11.0,  # 國泰金
            '2886': 10.0,  # 兆豐金
            '2891': 11.0,  # 中信金
            
            # 傳產
            '1301': 10.0,  # 台塑
            '1303': 9.0,   # 南亞
            '1326': 8.0,   # 台化
            
            # 電信業
            '2412': 16.0,  # 中華電
            '3045': 12.0,  # 台灣大
            '4904': 10.0,  # 遠傳
        }
        
        return industry_roe.get(stock_id, 15.0)  # 預設 15%


# 測試程式
if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # 設定日誌
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 建立計算器
    calculator = ROECalculator()
    
    # 測試產業平均 ROE
    test_stocks = ['2330', '2454', '2881', '1301', '9999']
    
    print("測試產業平均 ROE:")
    print("-" * 40)
    for stock_id in test_stocks:
        roe = calculator.get_industry_average_roe(stock_id)
        print(f"{stock_id}: {roe}%")
    
    print("\n測試備案機制:")
    print("-" * 40)
    for stock_id in test_stocks:
        roe = calculator.get_roe_with_fallback(stock_id)
        print(f"{stock_id}: {roe}%")