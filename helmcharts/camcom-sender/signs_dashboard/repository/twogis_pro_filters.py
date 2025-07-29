from typing import Type

from signs_dashboard.models.twogis_pro_filters import Base

PRIORITY = 'priority'


class TwogisProFiltersRepository:

    def __init__(self, session_factory):
        self.session_factory = session_factory

    def get_objects(self, model: Type[Base]) -> list[Base]:
        with self.session_factory() as session:
            return session.query(model).all()

    def add_or_update_object(self, target_obj: Base):
        with self.session_factory() as session:
            session.merge(target_obj)
            session.commit()
