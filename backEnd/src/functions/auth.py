from elasticsearch import NotFoundError
from passlib.context import CryptContext
from fastapi import HTTPException
from fastapi.security import OAuth2PasswordBearer
from ..database import es
from ...src.dto.user import UserInDB

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


# 2. Rota para upload dos arquivos, pensar em algo no máx de 10gb
# I can update more than 1 file per time, but max 10gb
# Need to create a new index for files
def upload_file(file):
    try:
        es.index(index="files", id=file.filename, body=file.file)
    except:
        raise HTTPException(status_code=400, detail="File too big")
    return file 


# 3. Rota de Busca rapida(colocar 50 elementos)
def search(query):
    return es.search(index="users", body=query)


# 4. Rota de Busca de Estado/Mês/Ano Mostrar Tnato PA/RD - Qtd de Registro - Total de gasto
def search_state(state, month, year):
    return es.search(index="users", body={"query": {"match": {"state": state, "month": month, "year": year}}})