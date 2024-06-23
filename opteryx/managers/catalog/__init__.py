def catalog_factory():
    from opteryx import config

    if config.DATA_CATALOG_PROVIDER is None or config.DATA_CATALOG_PROVIDER.upper() == "TARCHIA":
        from opteryx.managers.catalog.tarchia_provider import TarchiaCatalogProvider

        return TarchiaCatalogProvider()
    else:
        from opteryx.managers.catalog.null_provider import NullCatalogProvider

        return NullCatalogProvider()
