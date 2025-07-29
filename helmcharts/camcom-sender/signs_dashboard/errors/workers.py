class ParseMessageError(Exception):
    def __init__(self, message: str):
        self.message = message

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f'{class_name}(Error handle message, description = {self.message})'
