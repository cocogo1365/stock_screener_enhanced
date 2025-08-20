"""
詳細日誌系統設定
用於追蹤所有執行細節、API 請求、回應和錯誤
"""
import logging
import logging.handlers
import os
from datetime import datetime
import json
import traceback
from typing import Any, Dict

class DetailedFormatter(logging.Formatter):
    """自訂格式化器，提供更詳細的格式"""
    
    def format(self, record):
        # 添加額外的上下文資訊
        if hasattr(record, 'context'):
            record.msg = f"{record.msg}\n    Context: {json.dumps(record.context, ensure_ascii=False, indent=2)}"
        
        if hasattr(record, 'api_request'):
            record.msg = f"{record.msg}\n    Request: {json.dumps(record.api_request, ensure_ascii=False, indent=2)}"
            
        if hasattr(record, 'api_response'):
            record.msg = f"{record.msg}\n    Response: {json.dumps(record.api_response, ensure_ascii=False, indent=2)[:500]}"
        
        return super().format(record)

class DetailedLogger:
    """詳細日誌記錄器"""
    
    def __init__(self, name: str, log_dir: str = "logs"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # 創建日誌目錄
        os.makedirs(log_dir, exist_ok=True)
        
        # 日誌檔案名稱（包含時間戳）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"detailed_{name}_{timestamp}.log")
        
        # 檔案處理器（記錄所有層級）
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        
        # 詳細格式
        detailed_format = (
            "%(asctime)s | %(levelname)-8s | %(name)s | "
            "%(filename)s:%(lineno)d | %(funcName)s | %(message)s"
        )
        file_handler.setFormatter(DetailedFormatter(detailed_format))
        
        # 控制台處理器（只顯示 INFO 以上）
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        simple_format = "%(asctime)s | %(levelname)-8s | %(message)s"
        console_handler.setFormatter(logging.Formatter(simple_format))
        
        # 清除現有處理器並添加新的
        self.logger.handlers.clear()
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.log_file = log_file
        self.logger.info(f"=== 詳細日誌系統啟動 ===")
        self.logger.info(f"日誌檔案: {log_file}")
    
    def log_api_request(self, url: str, params: Dict = None, headers: Dict = None):
        """記錄 API 請求"""
        extra = {
            'api_request': {
                'url': url,
                'params': params,
                'headers': {k: v for k, v in (headers or {}).items() 
                          if k.lower() not in ['authorization', 'api-key', 'token']}
            }
        }
        self.logger.debug(f"API 請求: {url}", extra=extra)
    
    def log_api_response(self, url: str, status_code: int, response_data: Any):
        """記錄 API 回應"""
        extra = {
            'api_response': {
                'url': url,
                'status_code': status_code,
                'data_sample': str(response_data)[:500] if response_data else None
            }
        }
        self.logger.debug(f"API 回應: {url} [{status_code}]", extra=extra)
    
    def log_data_processing(self, stock_id: str, data_type: str, data: Dict):
        """記錄資料處理過程"""
        extra = {
            'context': {
                'stock_id': stock_id,
                'data_type': data_type,
                'data_keys': list(data.keys()) if isinstance(data, dict) else None,
                'data_sample': {k: v for k, v in (data or {}).items() 
                              if k in ['eps', 'roe', 'trust_holding', 'error']}
            }
        }
        self.logger.debug(f"處理資料: {stock_id} - {data_type}", extra=extra)
    
    def log_error_with_trace(self, message: str, error: Exception):
        """記錄錯誤與完整追蹤"""
        trace = traceback.format_exc()
        self.logger.error(f"{message}\n錯誤類型: {type(error).__name__}\n錯誤訊息: {str(error)}\n追蹤:\n{trace}")
    
    def log_screening_process(self, stock_id: str, conditions: Dict, result: Dict):
        """記錄篩選過程"""
        # 計算啟用的條件數（處理不同類型的值）
        enabled_count = 0
        for k, v in conditions.items():
            if isinstance(v, dict) and v.get('enabled'):
                enabled_count += 1
            elif isinstance(v, bool) and v:
                enabled_count += 1
            elif k.startswith('market_') and v:
                enabled_count += 1
        
        extra = {
            'context': {
                'stock_id': stock_id,
                'enabled_conditions': enabled_count,
                'matched_conditions': result.get('matched_count', 0),
                'passed': result.get('passed', False),
                'key_values': {
                    'eps': result.get('values', {}).get('eps'),
                    'roe': result.get('values', {}).get('roe'),
                    'trust_holding': result.get('values', {}).get('trust_holding')
                }
            }
        }
        self.logger.info(f"篩選結果: {stock_id}", extra=extra)
    
    def get_log_file_path(self):
        """取得日誌檔案路徑"""
        return self.log_file

# 全域日誌實例
_logger_instance = None

def get_detailed_logger(name: str = "stock_screener") -> DetailedLogger:
    """取得或建立詳細日誌記錄器"""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = DetailedLogger(name)
    return _logger_instance

def setup_module_logging():
    """設定所有模組的詳細日誌"""
    # 設定各模組的日誌層級
    logging.getLogger("src.enhanced_data_fetcher").setLevel(logging.DEBUG)
    logging.getLogger("src.complete_screening_engine").setLevel(logging.DEBUG)
    logging.getLogger("src.real_data_integration_final").setLevel(logging.DEBUG)
    logging.getLogger("src.technical_calculator").setLevel(logging.DEBUG)
    
    # 設定 requests 函式庫的日誌
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

if __name__ == "__main__":
    # 測試日誌系統
    logger = get_detailed_logger("test")
    
    logger.logger.info("測試 INFO 訊息")
    logger.logger.debug("測試 DEBUG 訊息")
    
    logger.log_api_request(
        "https://api.example.com/data",
        params={"stock_id": "2330", "date": "2024-01-01"},
        headers={"Authorization": "Bearer SECRET", "Content-Type": "application/json"}
    )
    
    logger.log_api_response(
        "https://api.example.com/data",
        200,
        {"data": [{"eps": 39.2, "roe": 28.5}]}
    )
    
    logger.log_data_processing(
        "2330",
        "financial",
        {"eps": 39.2, "roe": 28.5, "trust_holding": 0.8}
    )
    
    try:
        1 / 0
    except Exception as e:
        logger.log_error_with_trace("測試錯誤處理", e)
    
    print(f"\n日誌已儲存至: {logger.get_log_file_path()}")