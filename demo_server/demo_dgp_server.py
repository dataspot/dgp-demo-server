import os

from dgp_server.blueprint import DgpServer

from dataflows import Flow, add_computed_field, printer, duplicate, \
    set_type, set_primary_key, dump_to_sql, add_field, join_with_self
from dgp.core import Context, Config, BaseDataGenusProcessor
from dgp.taxonomies import Taxonomy
from dgp.config.consts import RESOURCE_NAME


class ConfigStorerDGP(BaseDataGenusProcessor):

    def __init__(self, config, context, lazy_engine):
        super().__init__(config, context)
        self.lazy_engine = lazy_engine

    def collate_values(self, fields):
        def func(row):
            return dict((f, row[f]) for f in fields)
        return func

    def flow(self):
        taxonomy = self.context.taxonomy
        txn_config = taxonomy.config
        fmt_str = [taxonomy.title + ' for:']
        fields = txn_config['key-fields']
        for f in fields:
            for ct in taxonomy.column_types:
                if ct['name'] == f:
                    fmt_str.append(
                        '%s: "{%s}",' % (ct['title'], f.replace(':', '-'))
                    )
                    break
        fmt_str = ' '.join(fmt_str)
        fields = [
            ct.replace(':', '-')
            for ct in fields
        ]
        all_fields = ['_source'] + fields

        TARGET = 'configurations'
        saved_config = self.config._unflatten()
        saved_config.setdefault('publish', {})['allowed'] = False

        return Flow(
            duplicate(RESOURCE_NAME, TARGET),
            join_with_self(
                TARGET,
                all_fields,
                dict((f, {}) for f in all_fields),
            ),
            add_computed_field(
                [
                    dict(
                        operation='format',
                        target='snippets',
                        with_=fmt_str
                    ),
                    dict(
                        operation='constant',
                        target='key_values',
                        with_=None
                    ),
                ],
                resources=TARGET
            ),
            add_field('config', 'object', saved_config, resources=TARGET),
            add_field('fields', type='object', 
                      default=self.collate_values(fields), resources=TARGET),
            join_with_self(
                TARGET,
                ['_source'],
                dict(
                    source=dict(name='_source'),
                    config={},
                    key_values=dict(aggregate='array'),
                    snippets=dict(aggregate='array'),
                )
            ),
            set_type('source', type='string'),
            set_type('config', type='object'),
            set_type('key_values', type='array'),
            set_type('snippets', type='array'),
            set_primary_key(['source']),
            dump_to_sql(
                dict([
                    (TARGET, {
                        'resource-name': TARGET,
                        'mode': 'update'
                    })
                ]),
                engine=self.lazy_engine(),
            ),
        )


class DemoDgpServer(DgpServer):

    def __init__(self):
        super().__init__(
            os.environ.get('BASE_PATH', '/var/dgp'),
            os.environ.get('DATABASE_URL', 'postgresql://postgres:123456@postgres/postgres'),
        )

    def loader_dgps(self, config: Config, context: Context):
        return [
        ]

    def publish_flow(self, config: Config, context: Context):
        super_dgps = super().publish_flow(config, context)
        super_dgps.append(
            ConfigStorerDGP(config, context, self.lazy_engine())
        )
        return super_dgps
