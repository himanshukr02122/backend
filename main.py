from fastapi import FastAPI, UploadFile, File, Body, Depends
from sqlalchemy import create_engine, text
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from helper import validate_dag, PipelineModel
from starlette import status

app = FastAPI()
origins = [
    "http://localhost:3000",  # React dev server
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = "mysql+pymysql://root:Him%400646@localhost:3306/assessment"
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS node_codes (
            node_id VARCHAR(255) PRIMARY KEY,
            code TEXT
        )
    """))

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.post('/pipelines/parse', status_code=status.HTTP_200_OK)
def parse_pipeline(pipeline: PipelineModel = Depends(validate_dag)):
    return {
        "num_nodes": len(pipeline.nodes),
        "num_edges": len(pipeline.edges),
        "is_dag": True
    }

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):

    if not file.filename.endswith(".csv"):
        return {"error": "Only CSV files allowed"}

    df = pd.read_csv(file.file)

    # Store in MySQL
    df.to_sql("uploaded_data", con=engine, if_exists="replace", index=False)

    # Return preview (first 10 rows)
    preview = df.head(10).to_dict(orient="records")

    return {
        "message": "File uploaded successfully",
        "columns": list(df.columns),
        "preview": preview,
        "total_rows": len(df)
    }

@app.post("/nodes/{node_id}/code")
def save_code(node_id: str, payload: dict = Body(...)):
    code = payload.get("code")

    # Validate python syntax
    try:
        compile(code, "<string>", "exec")
    except Exception as e:
        return {"error": str(e)}

    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO node_codes (node_id, code)
                VALUES (:node_id, :code)
                ON DUPLICATE KEY UPDATE code = :code
            """),
            {"node_id": node_id, "code": code}
        )
        conn.commit()

    return {"code": code}

@app.get("/nodes/{node_id}/code")
def get_code(node_id: str):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT code FROM node_codes WHERE node_id = :node_id"),
            {"node_id": node_id}
        ).fetchone()

    if not result:
        return {"code": None}

    return {"code": result[0]}