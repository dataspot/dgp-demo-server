from dataflows import Flow, add_field

from dgp.core.base_enricher import ColumnTypeTester, enrichments_flows, DuplicateRemover
from dgp.config.consts import RESOURCE_NAME


class SelectLatestProcessEnricher(DuplicateRemover):

    ORDER_BY_KEY = '{info-name}:{info-kind}'


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
                columnType='location:geojson',
                resources=RESOURCE_NAME
            ),
        )


class CreateLatLon(ColumnTypeTester):

    REQUIRED_COLUMN_TYPES = ['location:latlon']
    PROHIBITED_COLUMN_TYPES = ['location:lon', 'location:lat']

    def conditional(self):

        return Flow(
            add_field(
                'location-lat',
                'number',
                default=lambda r: r['location-latlon'].split(',')[0],
                columnType='location:lat',
                resources=RESOURCE_NAME
            ),
            add_field(
                'location-lon',
                'number',
                default=lambda r: r['location-latlon'].split(',')[1],
                columnType='location:lon',
                resources=RESOURCE_NAME
            ),
        )


class CreateAddress(ColumnTypeTester):

    REQUIRED_COLUMN_TYPES = []
    PROHIBITED_COLUMN_TYPES = ['location:address']

    def conditional(self):

        def address(row):
            return ' '.join(
                str(row[x])
                for x in (
                    'location-street-number',
                    'location-street-name',
                    'location-borough-name',
                    'location-city-name',
                    'location-region-name',
                    'location-country-name',
                )
                if x in row and row[x]
            )

        return Flow(
            add_field(
                'location-address',
                'string',
                default=address,
                columnType='location:address',
                resources=RESOURCE_NAME
            ),
        )


def flows(config, context):
    return enrichments_flows(
        config, context,
        CreateLatLon,
        CreateGeoJson,
        CreateAddress,
        SelectLatestProcessEnricher,
    )
