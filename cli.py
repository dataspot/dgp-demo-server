import sys

from dataflows import Flow, dump_to_path

from dgp.core import Config, Context
from dgp.genera.simple import SimpleDGP
from dgp.taxonomies import TaxonomyRegistry


def main():
    config = Config(sys.argv[1] if len(sys.argv) > 1 else 'dgp.yaml')
    taxonomy_registry = TaxonomyRegistry('taxonomies/index.yaml')
    context = Context(config, taxonomy_registry)

    dgp = SimpleDGP(config, context)
    ret = dgp.analyze()
    if not ret:
        print('Errors:', '\n\t - '.join([str(x) for x in dgp.errors]))
        sys.exit(0)

    flow = dgp.flow()
    flow = Flow(
        flow,
        dump_to_path('output')
    )
    flow.process()

    print('----')
    print('Success:', ret)


if __name__ == '__main__':
    main()

