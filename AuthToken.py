from dataclasses import dataclass

@dataclass
class Auth:
    device_code: str
    access_token: str
    refresh_token: str
    