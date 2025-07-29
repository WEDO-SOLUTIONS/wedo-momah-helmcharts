class ImageReadError(Exception):
    def __init__(self, key: str):
        self.key = key

    def __repr__(self) -> str:
        class_name = self.__class__.__name__
        return f'{class_name}(Error handle image, key = {self.key})'
