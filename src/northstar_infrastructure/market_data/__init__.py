"""Market data infrastructure adapters."""

from northstar_infrastructure.market_data.sqlite_historical_market_data import (
    HistoricalStorageError,
    SQLiteHistoricalMarketDataRepository,
    SQLiteHistoricalMarketDataStore,
)
from northstar_infrastructure.market_data.sqlite_schema import (
    HISTORICAL_MARKET_DATA_SCHEMA,
    initialize_historical_market_data_schema,
)
from northstar_infrastructure.market_data.yahoo_finance import YahooFinanceMarketObservationSource
from northstar_infrastructure.market_data.yahoo_historical_market_data import (
    HistoricalMarketDataSourceError,
    YahooHistoricalMarketDataSource,
)

__all__ = [
    "HISTORICAL_MARKET_DATA_SCHEMA",
    "HistoricalMarketDataSourceError",
    "HistoricalStorageError",
    "SQLiteHistoricalMarketDataRepository",
    "SQLiteHistoricalMarketDataStore",
    "YahooFinanceMarketObservationSource",
    "YahooHistoricalMarketDataSource",
    "initialize_historical_market_data_schema",
]
