# 台股智能篩選系統 - 增強版

## 檔案說明

### 主要檔案
- `stock_screener_enhanced.py` - 主程式 (GUI介面)
- `啟動篩選系統.bat` - Windows 執行檔

### 核心模組 (src/)
- `enhanced_data_fetcher.py` - 資料擷取器
- `complete_screening_engine.py` - 篩選引擎
- `technical_calculator.py` - 技術指標計算
- `scoring_system.py` - 評分系統
- `excel_exporter.py` - Excel 匯出

### 設定檔案
- `config.json` - 篩選參數設定
- `api_config.json` - API 金鑰設定
- `real_stock_list.csv` - 股票清單 (2933檔)

### 工具程式
- `fetch_all_taiwan_stocks.py` - 更新股票清單
- `diagnostic_fix.py` - 系統診斷工具
- `test_complete_fix.py` - 系統測試

## 使用方法

1. **首次使用**
   - 執行 `啟動篩選系統.bat`
   - 系統會自動安裝相依套件

2. **設定 API Token**
   - 在 GUI 中輸入 FinMind API Token
   - 或編輯 `api_config.json`

3. **更新股票清單**
   ```bash
   python fetch_all_taiwan_stocks.py
   ```

4. **執行篩選**
   - 設定篩選條件
   - 點擊「開始篩選」

5. **診斷問題**
   ```bash
   python diagnostic_fix.py
   ```

## 注意事項

- 需要 Python 3.8 以上版本
- 需要網路連線以取得股票資料
- 首次執行會較慢（下載資料）
- 篩選結果會儲存在 reports/ 目錄

## 問題排除

如果遇到問題，請依序執行：
1. `python diagnostic_fix.py` - 診斷問題
2. `python test_complete_fix.py` - 測試系統
3. 檢查 `logs/` 目錄中的日誌檔案
