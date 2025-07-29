import logging
from contextlib import contextmanager
from socket import gethostname

from sqlalchemy import create_engine, orm
from sqlalchemy.ext.declarative import declarative_base

logger = logging.getLogger(__name__)

Base = declarative_base()
MAX_APPNAME_LENGTH = 64


class Database:
    def __init__(self, connection_url: str, pool_size: int, pool_max_overflow: int) -> None:
        self._engine = create_engine(
            connection_url,
            echo=False,
            pool_pre_ping=True,
            connect_args={'application_name': gethostname()[:MAX_APPNAME_LENGTH]},
            pool_size=pool_size,
            max_overflow=pool_max_overflow,
        )
        self._session_factory = orm.scoped_session(
            orm.sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self._engine,
            ),
        )
        Base.metadata.create_all(self._engine)

    @contextmanager
    def session(self, expire_on_commit: bool = True):
        session: orm.Session = self._session_factory()
        session.expire_on_commit = expire_on_commit
        try:
            yield session
        except Exception:
            logger.exception('Session rollback because of exception')
            session.rollback()
            raise
        finally:
            session.close()
