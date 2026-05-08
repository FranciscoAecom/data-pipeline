class ValidationSession:
    def __init__(self):
        self.attribute_mappings = {}
        self.summary = {
            "fields": {},
            "relations": {},
        }

    def reset(self):
        self.attribute_mappings.clear()
        self.summary["fields"].clear()
        self.summary["relations"].clear()


_DEFAULT_VALIDATION_SESSION = ValidationSession()


def reset_validate_attribute_mappings():
    _DEFAULT_VALIDATION_SESSION.reset()


def validation_session_or_default(validation_session=None):
    return validation_session or _DEFAULT_VALIDATION_SESSION


__all__ = [
    "ValidationSession",
    "reset_validate_attribute_mappings",
    "validation_session_or_default",
]
