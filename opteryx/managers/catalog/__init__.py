# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


def catalog_factory():
    from opteryx import config

    if config.DATA_CATALOG_PROVIDER is None or config.DATA_CATALOG_PROVIDER.upper() == "TARCHIA":
        from opteryx.managers.catalog.tarchia_provider import TarchiaCatalogProvider

        return TarchiaCatalogProvider()
    else:
        from opteryx.managers.catalog.null_provider import NullCatalogProvider

        return NullCatalogProvider()
