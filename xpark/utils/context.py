CURR_EVALUATOR_BACKEND = None


class Override(object):
    def __init__(self, evaluator_backend=None):
        self.evaluator_backend = evaluator_backend

    def __enter__(self):
        if self.evaluator_backend is not None:
            global CURR_EVALUATOR_BACKEND
            CURR_EVALUATOR_BACKEND = self.evaluator_backend

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.evaluator_backend is not None:
            global CURR_EVALUATOR_BACKEND
            CURR_EVALUATOR_BACKEND = None