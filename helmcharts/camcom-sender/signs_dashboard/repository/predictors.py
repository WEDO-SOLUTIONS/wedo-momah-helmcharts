from signs_dashboard.models.predictor import Predictor


class PredictorsRepository:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    def all(self):
        with self.session_factory() as sess:
            return sess.query(Predictor).order_by(Predictor.name.asc()).all()

    def upsert(self, predictor: Predictor):
        with self.session_factory() as sess:
            sess.merge(predictor)
            sess.commit()
