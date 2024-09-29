# User and Token models
from pydantic import BaseModel


class User(BaseModel):
    username: str
    full_name: str = None
    email: str = None
    disabled: bool = None

class UserInDB(User):
    hashed_password: str
