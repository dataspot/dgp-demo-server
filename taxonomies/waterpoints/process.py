from dataflows import Flow, add_field

from dgp.core.base_enricher import ColumnTypeTester, enrichments_flows, DuplicateRemover
from dgp.config.consts import RESOURCE_NAME


class SelectLatestProcessEnricher(DuplicateRemover):

    ORDER_BY_KEY = '{location-lat}:{location-lon}'


class CreateGeoJson(ColumnTypeTester):

    REQUIRED_COLUMN_TYPES = ['location:lon', 'location:lat']
    PROHIBITED_COLUMN_TYPES = ['location:geojson']

    def conditional(self):

        def geojson(row):
            return '{"type": "Point","coordinates": [%s, %s]}' %\
                (row['location-lon'], row['location-lat'])

        return Flow(
            add_field(
                'location-geojson',
                'string',
                default=geojson,
                resources=RESOURCE_NAME
            ),
        )


def flows(config, context):
    return enrichments_flows(
        config, context,
        CreateGeoJson,
        SelectLatestProcessEnricher,
    )
