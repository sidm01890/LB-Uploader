from enum import Enum


class DataSource(Enum):
    """Data source enumeration matching devyani_python structure"""
    STORE_MASTER = "STORE_MASTER"
    POS_ORDERS = "POS_ORDERS"
    TRM_MPR = "TRM_MPR"
    PHONEPE_MPR = "PHONEPE_MPR"
    HDFC_MPR = "HDFC_MPR"
    AMEX_MPR = "AMEX_MPR"
    YESBANK_BS = "YESBANK_BS"
    HSBC_BS = "HSBC_BS"
    ZOMATO = "ZOMATO"
    SWIGGY = "SWIGGY"
    MAGICPIN = "MAGICPIN"
    ZOMATO_MAPPING = "ZOMATO_MAPPING"
    SWIGGY_MAPPING = "SWIGGY_MAPPING"
    EXCEL_DB_MAPPING = "EXCEL_DB_MAPPING"
    CMS_MPR = "CMS_MPR"

