from elasticsearch import NotFoundError
from passlib.context import CryptContext
from fastapi import HTTPException
from fastapi.security import OAuth2PasswordBearer
from ..database import es
from src.dto.user import UserInDB

# Password hashing context
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Security scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# Utility functions
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_user(username: str):
    try:
        response = es.get(index="users", id=username)
        user_dict = response['_source']
        return UserInDB(**user_dict)
    except NotFoundError:
        return None

def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user or not verify_password(password, user.hashed_password):
        return False
    return user

def create_user(user: UserInDB):
    if get_user(user.username) is not None:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    user_dict = user.dict()
    user_dict['hashed_password'] = pwd_context.hash(user_dict['hashed_password'])
    es.index(index="users", id=user.username, body=user_dict)