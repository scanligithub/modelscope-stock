import pyarrow as pa

class AShareDataSchema:
    DATE = 'date'
    CODE = 'code'
    
    OPEN = 'open'
    HIGH = 'high'
    LOW = 'low'
    CLOSE = 'close'
    VOLUME = 'volume'
    AMOUNT = 'amount'
    TURN = 'turn'
    PCT_CHG = 'pctChg'
    PE_TTM = 'peTTM'
    PB_MRQ = 'pbMRQ'
    ADJ_FACTOR = 'adjustFactor'
    IS_ST = 'isST'

    NET_FLOW = 'net_amount'
    MAIN_FLOW = 'main_net'
    SUPER_FLOW = 'super_net'
    LARGE_FLOW = 'large_net'
    MEDIUM_FLOW = 'medium_net'
    SMALL_FLOW = 'small_net'

    NAME = 'name'
    TYPE = 'type'
