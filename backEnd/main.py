import json
from elastic_transport import ListApiResponse
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm

from backEnd.src.functions.databases import search_database, search_state_database

# from functions.auth import authenticate_user, create_user, get_user

from .src.functions.auth import authenticate_user, create_user, get_user, oauth2_scheme
from .src.dto.user import User, UserInDB
from .src.dto.token import Token

from .src.database import es

# Initialize FastAPI app
app = FastAPI()


#default 
@app.get("/")
async def read_root():
    return {"message": "Hello World"}

# Registration endpoint
@app.post("/register", response_model=User)
async def register(user: UserInDB):
    create_user(user)
    return user


# User info endpoint
@app.get("/users/me", response_model=User)
async def read_users_me(token: str = Depends(oauth2_scheme)):
    user = get_user(token)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user


# 1. Rota de autenticação

# Authentication endpoint
@app.post("/token", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    return {"access_token": user.username, "token_type": "bearer"}


# 2. Rota para upload dos arquivos, pensar em algo no máx de 10gb
@app.post("/upload", response_model=User)
async def upload_file(file):
    return upload_file(file)

# 3. Rota de Busca rapida(colocar 50 elementos)
@app.get("/search")
async def search():
    try:
        response = await search_database()
        return JSONResponse(content=response.body)
    except Exception as e:
        return {"error": str(e)}

# 4. Rota de Busca de Estado/Mês/Ano Mostrar Tnato PA/RD - Qtd de Registro - Total de gasto
@app.get("/search/state")
async def search_state():
    try:
        response = await search_state_database()

        body_response = response.body

        data = {
            "ALL": {
                "qtde": body_response["aggregations"]["total_documents"]["value"],
                 "total": round(response["aggregations"]["total_sum"]["value"],2)
            },
            
            "PA": {
                "qtde": body_response["aggregations"]["tipos"]["buckets"][0]["total_documents"]["value"], 
                "total": round(body_response["aggregations"]["tipos"]["buckets"][0]["total_sum"]["value"],2)
                },
            "RD": {
                "qtde": body_response["aggregations"]["tipos"]["buckets"][1]["total_documents"]["value"], 
                "total": round(body_response["aggregations"]["tipos"]["buckets"][1]["total_sum"]["value"],2)},
        }
        return JSONResponse(content=data)
    except Exception as e:
        return {"error": str(e)}    
    
# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
