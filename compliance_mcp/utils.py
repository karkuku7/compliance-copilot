import importlib.metadata
import re

LOGIN_REGEX = r"^[a-z]{3,8}$"


def validate_amazon_login(login: str) -> None:
    if not re.match(LOGIN_REGEX, login):
        raise ValueError(f"Invalid login syntax. Must match regex {LOGIN_REGEX}")


def get_package_version() -> str:
    try:
        return importlib.metadata.version("compliance-copilot")
    except BaseException:
        raise RuntimeError("Could not determine the version of package")
