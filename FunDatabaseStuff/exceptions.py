class NonExistentTableError(Exception):
    pass


class NonExistentColumnError(Exception):
    pass


class ColumnError(Exception):
    pass


class InvalidCreateError(Exception):
    pass


class ModelNonExistentError(Exception):
    pass
