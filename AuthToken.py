from dataclasses import dataclass

@dataclass
class Auth:
    access_token: str
    refresh_token: str
    