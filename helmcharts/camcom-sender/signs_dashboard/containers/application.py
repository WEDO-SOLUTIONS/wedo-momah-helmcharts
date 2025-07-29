from dependency_injector import containers, providers

from signs_dashboard.containers.pg_repositories import PgRepositories
from signs_dashboard.containers.services_container import ServicesContainer


class Application(containers.DeclarativeContainer):
    config = providers.Configuration()

    pg_repositories: PgRepositories = providers.Container(
        PgRepositories,
        config=config,
    )

    services: ServicesContainer = providers.Container(
        ServicesContainer,
        config=config,
        pg_repositories=pg_repositories,
    )
