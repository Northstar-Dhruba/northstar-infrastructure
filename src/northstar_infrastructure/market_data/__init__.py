"""Market data infrastructure adapters."""

from northstar_infrastructure.market_data.databento_futures_historical_market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    DatabentoFuturesHistoricalMarketDataSourceError,
    FuturesTradingSessionInProgressError,
)
from northstar_infrastructure.market_data.exchange_calendar_futures_session import (
    ExchangeCalendarFuturesTradingSessionResolver,
)
from northstar_infrastructure.market_data.historical_futures_contract_reference import (
    SYNTHETIC_REFERENCE_AUTHORITY_PREFIX,
    HistoricalFuturesCalendarFinding,
    HistoricalFuturesCalendarStatus,
    HistoricalFuturesContractCutoffSource,
    HistoricalFuturesContractReference,
    HistoricalFuturesContractReferenceCatalog,
    HistoricalFuturesContractReferenceError,
    HistoricalFuturesLotSize,
    HistoricalFuturesReferenceCalendarCheck,
    HistoricalFuturesReferenceKind,
    HistoricalFuturesReferenceSource,
    HistoricalFuturesReferenceUnavailableError,
    HistoricalFuturesTickSize,
    check_historical_futures_reference_calendar,
)
from northstar_infrastructure.market_data.historical_futures_reference_evidence import (
    PRODUCTION_EVIDENCE_AUTHORITY_PREFIX,
    TEST_EVIDENCE_AUTHORITY_PREFIX,
    HistoricalFuturesAcceptedEvidenceRegistry,
    HistoricalFuturesAcceptedFact,
    HistoricalFuturesBoundarySemantics,
    HistoricalFuturesDocumentIdentity,
    HistoricalFuturesEvidenceCutoffSource,
    HistoricalFuturesEvidenceError,
    HistoricalFuturesEvidenceGate,
    HistoricalFuturesEvidenceRefusedError,
    HistoricalFuturesFactCategory,
    HistoricalFuturesOwnerAcceptance,
    HistoricalFuturesReferenceAuthority,
)
from northstar_infrastructure.market_data.nse_accepted_equity_session_evidence import (
    PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE,
    PRODUCTION_NSE_EQUITY_SESSION_CATALOG,
)
from northstar_infrastructure.market_data.nse_accepted_reference_evidence import (
    PRODUCTION_ACCEPTED_EVIDENCE,
)
from northstar_infrastructure.market_data.nse_equity_research_window_reference import (
    NO_APPROVED_APPLICABILITY_SOURCE,
    NoApprovedEquityListingApplicabilitySource,
    NSEEquityVenueDayReferenceSource,
)
from northstar_infrastructure.market_data.nse_equity_session_reference import (
    PRODUCTION_EQUITY_SESSION_AUTHORITY_PREFIX,
    TEST_EQUITY_SESSION_AUTHORITY_PREFIX,
    NSEEquityAcceptedSessionEvidence,
    NSEEquityAcceptedSessionFact,
    NSEEquityClosedDate,
    NSEEquityClosureKind,
    NSEEquityContinuousClose,
    NSEEquityContinuousCloseRule,
    NSEEquityExceptionReference,
    NSEEquityOpeningRule,
    NSEEquityOwnerAcceptance,
    NSEEquitySecurityGroup,
    NSEEquitySessionCatalog,
    NSEEquitySessionEvidenceError,
    NSEEquitySessionRequiresInstrumentApplicabilityError,
    NSEEquitySessionUnavailableError,
    NSEEquitySourceIdentity,
    NSEEquitySourceRole,
    NSEEquitySpecialSessionReference,
    NSEEquityTradingSessionResolver,
    NSEEquityUnsupportedClose,
    NSEEquityVenueDay,
    NSEEquityVenueDayStatus,
    NSEEquityVenueSessionReference,
)
from northstar_infrastructure.market_data.nse_futures_session import (
    NSEFuturesTradingSessionResolver,
)
from northstar_infrastructure.market_data.nse_option_expiration import (
    NSEOptionExpirationResolver,
)
from northstar_infrastructure.market_data.nse_option_session import (
    NSEOptionTradingSessionResolver,
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
from northstar_infrastructure.market_data.sqlite_option_chain_daily_bars import (
    SQLiteOptionChainDailyBarRepository,
)
from northstar_infrastructure.market_data.sqlite_option_daily_acquisition_store import (
    OptionOpenInterestCapture,
    OptionOpenInterestCaptureError,
    SQLiteOptionDailyAcquisitionStore,
)
from northstar_infrastructure.market_data.sqlite_option_historical_market_data import (
    OptionHistoricalStorageError,
    SQLiteOptionHistoricalMarketDataRepository,
    SQLiteOptionHistoricalMarketDataStore,
)
from northstar_infrastructure.market_data.sqlite_option_listed_contracts import (
    SQLiteOptionListedContractRepository,
)
from northstar_infrastructure.market_data.sqlite_option_listing_schema import (
    OPTION_LISTING_SNAPSHOTS_SCHEMA,
    OPTION_PROVIDER_LISTINGS_SCHEMA,
    initialize_option_listing_schema,
)
from northstar_infrastructure.market_data.sqlite_option_market_data_schema import (
    OPTION_OHLCV_SCHEMA,
    OPTION_PROVIDER_OPEN_INTEREST_SCHEMA,
    initialize_option_market_data_schema,
    initialize_option_provider_open_interest_schema,
)
from northstar_infrastructure.market_data.sqlite_option_provider_listings import (
    OptionListingConflictError,
    OptionListingStorageError,
    SQLiteOptionListingRepository,
    SQLiteOptionListingStore,
    StoredOptionProviderListing,
)
from northstar_infrastructure.market_data.sqlite_option_provider_open_interest import (
    OptionOpenInterestConflictError,
    OptionOpenInterestStorageError,
    ProviderOptionOpenInterest,
    SQLiteOptionProviderOpenInterestRepository,
    SQLiteOptionProviderOpenInterestStore,
)
from northstar_infrastructure.market_data.sqlite_schema import (
    HISTORICAL_MARKET_DATA_SCHEMA,
    initialize_historical_market_data_schema,
)
from northstar_infrastructure.market_data.synthetic_equity_listing_applicability import (
    InvalidSyntheticListingApplicabilityError,
    SyntheticListingApplicabilityRecord,
    SyntheticListingApplicabilitySource,
)
from northstar_infrastructure.market_data.synthetic_equity_session import (
    InvalidSyntheticEquitySessionReferenceError,
    SyntheticEquityPhaseHours,
    SyntheticEquitySessionReference,
    SyntheticEquitySpecialSession,
    SyntheticEquityTradingSessionResolver,
)
from northstar_infrastructure.market_data.upstox_candle_evidence import (
    MalformedUpstoxCandleEvidenceError,
    TruncatedUpstoxCandleEvidenceError,
    UpstoxCandleEvidenceError,
    UpstoxCandleEvidenceLog,
    UpstoxCandleEvidenceLogError,
    UpstoxDailyCandleEvidence,
    UpstoxDailyCandleEvidenceCollector,
    UpstoxDailyCandleEvidenceObservation,
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
from northstar_infrastructure.market_data.upstox_option_instrument_master import (
    UPSTOX_PROVIDER,
    UpstoxOptionInstrumentMaster,
    UpstoxOptionListing,
    UpstoxOptionMasterError,
    UpstoxOptionMasterSnapshot,
)
from northstar_infrastructure.market_data.upstox_option_native_daily_market_data import (
    OptionProviderListingNotStoredError,
    UpstoxOptionNativeDailyMarketDataSource,
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
    "MalformedUpstoxCandleEvidenceError",
    "NSEFuturesTradingSessionResolver",
    "NSEOptionExpirationResolver",
    "NO_APPROVED_APPLICABILITY_SOURCE",
    "NoApprovedEquityListingApplicabilitySource",
    "NSEEquityVenueDayReferenceSource",
    "InvalidSyntheticListingApplicabilityError",
    "SyntheticListingApplicabilityRecord",
    "SyntheticListingApplicabilitySource",
    "PRODUCTION_ACCEPTED_EQUITY_SESSION_EVIDENCE",
    "PRODUCTION_EQUITY_SESSION_AUTHORITY_PREFIX",
    "PRODUCTION_NSE_EQUITY_SESSION_CATALOG",
    "TEST_EQUITY_SESSION_AUTHORITY_PREFIX",
    "NSEEquityAcceptedSessionEvidence",
    "NSEEquityAcceptedSessionFact",
    "NSEEquityClosedDate",
    "NSEEquityClosureKind",
    "NSEEquityContinuousClose",
    "NSEEquityContinuousCloseRule",
    "NSEEquityExceptionReference",
    "NSEEquityOpeningRule",
    "NSEEquityOwnerAcceptance",
    "NSEEquitySecurityGroup",
    "NSEEquitySessionCatalog",
    "NSEEquitySessionEvidenceError",
    "NSEEquitySessionRequiresInstrumentApplicabilityError",
    "NSEEquitySessionUnavailableError",
    "NSEEquitySourceIdentity",
    "NSEEquitySourceRole",
    "NSEEquitySpecialSessionReference",
    "NSEEquityTradingSessionResolver",
    "NSEEquityUnsupportedClose",
    "NSEEquityVenueDay",
    "NSEEquityVenueDayStatus",
    "NSEEquityVenueSessionReference",
    "InvalidSyntheticEquitySessionReferenceError",
    "SyntheticEquityPhaseHours",
    "SyntheticEquitySessionReference",
    "SyntheticEquitySpecialSession",
    "SyntheticEquityTradingSessionResolver",
    "SQLiteFuturesHistoricalMarketDataRepository",
    "SQLiteFuturesHistoricalMarketDataStore",
    "SQLiteHistoricalMarketDataRepository",
    "SQLiteHistoricalMarketDataStore",
    "TruncatedUpstoxCandleEvidenceError",
    "UpstoxAccessBlockedError",
    "UpstoxAuthenticationError",
    "UpstoxCandleEvidenceError",
    "UpstoxCandleEvidenceLog",
    "UpstoxCandleEvidenceLogError",
    "UpstoxDailyCandleEvidence",
    "UpstoxDailyCandleEvidenceCollector",
    "UpstoxDailyCandleEvidenceObservation",
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
    "OPTION_LISTING_SNAPSHOTS_SCHEMA",
    "OPTION_PROVIDER_LISTINGS_SCHEMA",
    "OptionListingConflictError",
    "OptionListingStorageError",
    "SQLiteOptionListingRepository",
    "SQLiteOptionListingStore",
    "StoredOptionProviderListing",
    "UPSTOX_PROVIDER",
    "UpstoxOptionInstrumentMaster",
    "UpstoxOptionListing",
    "UpstoxOptionMasterError",
    "UpstoxOptionMasterSnapshot",
    "initialize_option_listing_schema",
    "NSEOptionTradingSessionResolver",
    "OPTION_OHLCV_SCHEMA",
    "OPTION_PROVIDER_OPEN_INTEREST_SCHEMA",
    "OptionHistoricalStorageError",
    "OptionOpenInterestConflictError",
    "OptionOpenInterestStorageError",
    "ProviderOptionOpenInterest",
    "SQLiteOptionHistoricalMarketDataRepository",
    "SQLiteOptionHistoricalMarketDataStore",
    "SQLiteOptionProviderOpenInterestRepository",
    "SQLiteOptionProviderOpenInterestStore",
    "initialize_option_market_data_schema",
    "initialize_option_provider_open_interest_schema",
    "OptionOpenInterestCapture",
    "OptionOpenInterestCaptureError",
    "OptionProviderListingNotStoredError",
    "SQLiteOptionDailyAcquisitionStore",
    "UpstoxOptionNativeDailyMarketDataSource",
    "SQLiteOptionChainDailyBarRepository",
    "SQLiteOptionListedContractRepository",
    "SYNTHETIC_REFERENCE_AUTHORITY_PREFIX",
    "HistoricalFuturesCalendarFinding",
    "HistoricalFuturesCalendarStatus",
    "HistoricalFuturesContractCutoffSource",
    "HistoricalFuturesContractReference",
    "HistoricalFuturesContractReferenceCatalog",
    "HistoricalFuturesContractReferenceError",
    "HistoricalFuturesLotSize",
    "HistoricalFuturesReferenceCalendarCheck",
    "HistoricalFuturesReferenceKind",
    "HistoricalFuturesReferenceSource",
    "HistoricalFuturesReferenceUnavailableError",
    "HistoricalFuturesTickSize",
    "check_historical_futures_reference_calendar",
    "PRODUCTION_ACCEPTED_EVIDENCE",
    "PRODUCTION_EVIDENCE_AUTHORITY_PREFIX",
    "TEST_EVIDENCE_AUTHORITY_PREFIX",
    "HistoricalFuturesAcceptedEvidenceRegistry",
    "HistoricalFuturesAcceptedFact",
    "HistoricalFuturesBoundarySemantics",
    "HistoricalFuturesDocumentIdentity",
    "HistoricalFuturesEvidenceCutoffSource",
    "HistoricalFuturesEvidenceError",
    "HistoricalFuturesEvidenceGate",
    "HistoricalFuturesEvidenceRefusedError",
    "HistoricalFuturesFactCategory",
    "HistoricalFuturesOwnerAcceptance",
    "HistoricalFuturesReferenceAuthority",
]
