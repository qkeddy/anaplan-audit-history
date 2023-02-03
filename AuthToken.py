# ===============================================================================
# Created:        3 Feb 2023
# @author:        Quinlan Eddy (Anaplan, Inc)
# Description:    Data Factory to store temporary variables
# ===============================================================================


from dataclasses import dataclass

@dataclass
class Auth:
    client_id: str
    device_code: str
    access_token: str
    refresh_token: str = "none"
    