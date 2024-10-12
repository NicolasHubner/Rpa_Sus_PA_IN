from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordRequestForm

# from functions.auth import authenticate_user, create_user, get_user

from src.functions.auth import authenticate_user, create_user, get_user, oauth2_scheme
from src.dto.user import User, UserInDB
from src.dto.token import Token



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

# Authentication endpoint
@app.post("/token", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    return {"access_token": user.username, "token_type": "bearer"}

# User info endpoint
@app.get("/users/me", response_model=User)
async def read_users_me(token: str = Depends(oauth2_scheme)):
    user = get_user(token)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
