from pydantic import BaseModel


class User(BaseModel):
    username: str
    full_name: str = None
    email: str = None
    disabled: bool = None

class UserRegister(User):
    password: str

class UserLogin(BaseModel):
    username: str
    password: str


class UserInDB(User):
    hashed_password: str