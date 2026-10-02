"""Market data infrastructure adapters."""

from northstar_infrastructure.market_data.databento_futures_historical_market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    DatabentoFuturesHistoricalMarketDataSourceError,
    FuturesTradingSessionInProgressError,
)
from northstar_infrastructure.market_data.exchange_calendar_futures_session import (
    ExchangeCalendarFuturesTradingSessionResolver,
)
from northstar_infrastructure.market_data.nse_futures_session import (
    NSEFuturesTradingSessionResolver,
)
from northstar_infrastructure.market_data.sqlite_futures_historical_market_data import (
    FuturesHistoricalStorageError,
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
)
from northstar_infrastructure.market_data.sqlite_futures_schema import (
    FUTURES_MARKET_DATA_SCHEMA,
    initialize_futures_market_data_schema,
)
from northstar_infrastructure.market_data.sqlite_historical_market_data import (
    HistoricalStorageError,
    SQLiteHistoricalMarketDataRepository,
    SQLiteHistoricalMarketDataStore,
)
from northstar_infrastructure.market_data.sqlite_schema import (
    HISTORICAL_MARKET_DATA_SCHEMA,
    initialize_historical_market_data_schema,
)
from northstar_infrastructure.market_data.upstox_futures_native_daily_market_data import (
    UpstoxFuturesNativeDailyMarketDataSource,
)
from northstar_infrastructure.market_data.upstox_http import (
    UpstoxAccessBlockedError,
    UpstoxAuthenticationError,
    UpstoxInstrumentResolutionError,
    UpstoxInvalidInstrumentKeyError,
    UpstoxMarketDataSourceError,
    UpstoxProviderUnavailableError,
)
from northstar_infrastructure.market_data.upstox_instrument_master import (
    UpstoxInstrumentMaster,
)
from northstar_infrastructure.market_data.yahoo_finance import YahooFinanceMarketObservationSource
from northstar_infrastructure.market_data.yahoo_historical_market_data import (
    HistoricalMarketDataSourceError,
    YahooHistoricalMarketDataSource,
)

__all__ = [
    "FUTURES_MARKET_DATA_SCHEMA",
    "HISTORICAL_MARKET_DATA_SCHEMA",
    "DatabentoFuturesHistoricalMarketDataSource",
    "DatabentoFuturesHistoricalMarketDataSourceError",
    "ExchangeCalendarFuturesTradingSessionResolver",
    "FuturesHistoricalStorageError",
    "FuturesTradingSessionInProgressError",
    "HistoricalMarketDataSourceError",
    "HistoricalStorageError",
    "NSEFuturesTradingSessionResolver",
    "SQLiteFuturesHistoricalMarketDataRepository",
    "SQLiteFuturesHistoricalMarketDataStore",
    "SQLiteHistoricalMarketDataRepository",
    "SQLiteHistoricalMarketDataStore",
    "UpstoxAccessBlockedError",
    "UpstoxAuthenticationError",
    "UpstoxFuturesNativeDailyMarketDataSource",
    "UpstoxInstrumentMaster",
    "UpstoxInstrumentResolutionError",
    "UpstoxInvalidInstrumentKeyError",
    "UpstoxMarketDataSourceError",
    "UpstoxProviderUnavailableError",
    "YahooFinanceMarketObservationSource",
    "YahooHistoricalMarketDataSource",
    "initialize_futures_market_data_schema",
    "initialize_historical_market_data_schema",
]
