from enum import StrEnum, auto


class YesNoEnum(StrEnum):
    YES = auto()
    NO = auto()


class TimeDatatypeEnum(StrEnum):
    EPOCH = auto()
    TIMESTAMP = auto()
    DATETIME = auto()
    DATE = auto()
    YYYYMMDD = auto()


class ViewBaseTypeEnum(StrEnum):
    STRING = auto()
    NUMBER = auto()
    TIME = auto()
    YESNO = auto()
    BIN = auto()
    TIER = auto()
    UNQUOTED = auto()


class JoinTypeEnum(StrEnum):
    LEFT_OUTER = auto()
    INNER = auto()
    FULL_OUTER = auto()
    CROSS = auto()


class JoinRelationshipEnum(StrEnum):
    MANY_TO_MANY = auto()
    MANY_TO_ONE = auto()
    ONE_TO_MANY = auto()
    ONE_TO_ONE = auto()


class LookMLFieldStructEnum(StrEnum):
    DIMENSION = auto()
    DIMENSION_GROUP = auto()
    MEASURE = auto()
    FILTER = auto()
    PARAMETER = auto()


class LookMLTimeframesEnum(StrEnum):
    TIME = auto()
    TIME_OF_DAY = auto()
    HOUR = auto()
    HOUR_OF_DAY = auto()
    MINUTE = auto()
    SECOND = auto()
    MILLISECOND = auto()
    MICROSECOND = auto()
    DATE = auto()
    WEEK = auto()
    DAY_OF_WEEK = auto()
    MONTH = auto()
    MONTH_NUM = auto()
    MONTH_NAME = auto()
    QUARTER = auto()
    QUARTER_OF_YEAR = auto()
    YEAR = auto()
    WEEK_OF_YEAR = auto()
    DAY_OF_YEAR = auto()
    RAW = auto()


class LookMLDashboardElementTypeEnum(StrEnum):
    # Cartesian charts
    LOOKER_COLUMN = auto()
    LOOKER_BAR = auto()
    LOOKER_SCATTER = auto()
    LOOKER_LINE = auto()
    LOOKER_AREA = auto()
    LOOKER_BOXPLOT = auto()
    LOOKER_WATERFALL = auto()
    # Pie and donut charts
    LOOKER_PIE = auto()
    LOOKER_DONUT_MULTIPLES = auto()
    # Progression charts
    LOOKER_FUNNEL = auto()
    LOOKER_TIMELINE = auto()
    # Text and tables
    TEXT = auto()
    LOOKER_GRID = auto()
    TABLE = auto()
    SINGLE_VALUE = auto()
    LOOKER_SINGLE_RECORD = auto()
    # Maps
    LOOKER_MAP = auto()
    LOOKER_GOOGLE_MAP = auto()
    LOOKER_GEO_COORDINATES = auto()
    LOOKER_GEO_CHOROPLETH = auto()
    LOOKER_WORDCLOUD = auto()


class LookMLMeasureTypeEnum(StrEnum):
    AVERAGE = auto()
    AVERAGE_DISTINCT = auto()
    COUNT = auto()
    COUNT_DISTINCT = auto()
    MAX = auto()
    MEDIAN = auto()
    MEDIAN_DISTINCT = auto()
    PERCENTILE = auto()
    SUM = auto()
    SUM_DISTINCT = auto()
