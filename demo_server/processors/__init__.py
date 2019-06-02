from dgp.core.base_enricher import DatapackageJoiner, DuplicateRemover


class MunicipalityNameToCodeEnricher(DatapackageJoiner):

    REQUIRED_COLUMN_TYPES = ['municipality:name']
    PROHIBITED_COLUMN_TYPES = ['municipality:code']
    REF_DATAPACKAGE = 'http://next.obudget.org/datapackages/lamas-municipal-data/datapackage.json'
    REF_KEY_FIELDS = ['name_municipality']
    REF_FETCH_FIELDS = ['symbol_municipality_2015']
    SOURCE_KEY_FIELDS = ['municipality-name']
    TARGET_FIELD_COLUMNTYPES = ['municipality:code']
