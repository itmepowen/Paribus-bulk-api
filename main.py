import csv
import uuid
import time
import asyncio
from io import StringIO
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import httpx

app = FastAPI(
    title="Hospital Bulk Processing API (Pro Edition)",
    description="Bulk processing system with WebSockets, Resume, and Validation.",
    version="2.0.0"
)

EXTERNAL_API_BASE_URL = "https://hospital-directory.onrender.com"
MAX_CSV_ROWS = 20

# --- In-Memory Database for Tracking & Resuming ---
# Structure: { batch_id: {"total": int, "processed": int, "failed": int, "rows": [...], "results": [...]} }
job_store: Dict[str, Dict[str, Any]] = {}

# --- WebSocket Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, batch_id: str):
        await websocket.accept()
        if batch_id not in self.active_connections:
            self.active_connections[batch_id] = []
        self.active_connections[batch_id].append(websocket)

    def disconnect(self, websocket: WebSocket, batch_id: str):
        if batch_id in self.active_connections:
            self.active_connections[batch_id].remove(websocket)

    async def broadcast(self, message: dict, batch_id: str):
        if batch_id in self.active_connections:
            for connection in self.active_connections[batch_id]:
                try:
                    await connection.send_json(message)
                except Exception:
                    pass

manager = ConnectionManager()

# --- Response Models ---
class HospitalResult(BaseModel):
    row: int
    hospital_id: Optional[int] = None
    name: str
    status: str
    error: Optional[str] = None

class BulkProcessResponse(BaseModel):
    batch_id: str
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    batch_activated: bool
    hospitals: List[HospitalResult]

# --- Helper Functions ---
async def validate_csv_content(file: UploadFile) -> List[dict]:
    """Reads and validates CSV rules, returning the rows if valid."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted.")
    
    content = await file.read()
    try:
        text_content = content.decode('utf-8')
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Invalid UTF-8 encoding.")
        
    csv_reader = csv.DictReader(StringIO(text_content))
    expected_columns = ["name", "address", "phone"]
    
    if not csv_reader.fieldnames or not all(col in expected_columns for col in ["name", "address"]):
        raise HTTPException(status_code=400, detail="CSV must contain 'name' and 'address'.")

    rows = list(csv_reader)
    if len(rows) == 0:
        raise HTTPException(status_code=400, detail="CSV is empty.")
    if len(rows) > MAX_CSV_ROWS:
        raise HTTPException(status_code=400, detail=f"Exceeds max {MAX_CSV_ROWS} rows.")
        
    return rows

async def create_hospital(client: httpx.AsyncClient, row_idx: int, row_data: dict, batch_id: str) -> HospitalResult:
    """Makes external API call and broadcasts progress."""
    payload = {
        "name": row_data.get("name", "").strip(),
        "address": row_data.get("address", "").strip(),
        "phone": row_data.get("phone", "").strip(),
        "creation_batch_id": batch_id
    }
    
    try:
        response = await client.post(f"{EXTERNAL_API_BASE_URL}/hospitals/", json=payload, timeout=60.0)
        response.raise_for_status()
        result = HospitalResult(row=row_idx, hospital_id=response.json().get("id"), name=payload["name"], status="created")
        job_store[batch_id]["processed"] += 1
    except httpx.HTTPError as e:
        error_msg = f"{type(e).__name__}: {str(e)}" if str(e) else type(e).__name__
        result = HospitalResult(row=row_idx, name=payload["name"], status="failed", error=error_msg)
        job_store[batch_id]["failed"] += 1

    # Broadcast real-time update
    await manager.broadcast({
        "processed": job_store[batch_id]["processed"],
        "failed": job_store[batch_id]["failed"],
        "total": job_store[batch_id]["total"]
    }, batch_id)
    
    return result

# --- Endpoints ---

@app.post("/hospitals/bulk/validate")
async def validation_endpoint(file: UploadFile = File(...)):
    """Bonus: Dedicated endpoint to validate CSV format before processing."""
    rows = await validate_csv_content(file)
    return {"valid": True, "total_rows_detected": len(rows), "message": "CSV format is valid and ready for processing."}

@app.post("/hospitals/bulk", response_model=BulkProcessResponse)
async def bulk_create_hospitals(file: UploadFile = File(...)):
    """Core logic with Job Tracking integration."""
    rows = await validate_csv_content(file)
    batch_id = str(uuid.uuid4())
    
    # Initialize Tracking Store
    job_store[batch_id] = {"total": len(rows), "processed": 0, "failed": 0, "rows": rows, "results": []}
    
    async with httpx.AsyncClient() as client:
        tasks = [create_hospital(client, idx + 1, row, batch_id) for idx, row in enumerate(rows)]
        results = await asyncio.gather(*tasks)
    
    job_store[batch_id]["results"] = results
    
    # Activation logic
    batch_activated = False
    if job_store[batch_id]["failed"] == 0:
        async with httpx.AsyncClient() as client:
            resp = await client.patch(f"{EXTERNAL_API_BASE_URL}/hospitals/batch/{batch_id}/activate")
            if resp.status_code == 200:
                batch_activated = True

    return BulkProcessResponse(
        batch_id=batch_id,
        total_hospitals=len(rows),
        processed_hospitals=job_store[batch_id]["processed"],
        failed_hospitals=job_store[batch_id]["failed"],
        batch_activated=batch_activated,
        hospitals=results
    )

@app.post("/hospitals/bulk/{batch_id}/resume", response_model=BulkProcessResponse)
async def resume_failed_batch(batch_id: str):
    """Bonus: Resume processing for hospitals that failed in a specific batch."""
    if batch_id not in job_store:
        raise HTTPException(status_code=404, detail="Batch ID not found in memory.")
    
    job = job_store[batch_id]
    if job["failed"] == 0:
        return {"message": "No failed hospitals to resume in this batch."}

    # Identify failed rows
    failed_results = [r for r in job["results"] if r.status == "failed"]
    job["failed"] -= len(failed_results) # Reset fail counter for retry
    
    async with httpx.AsyncClient() as client:
        tasks = []
        for failed_res in failed_results:
            row_data = job["rows"][failed_res.row - 1]
            tasks.append(create_hospital(client, failed_res.row, row_data, batch_id))
            
        new_results = await asyncio.gather(*tasks)

    # Update state
    for new_res in new_results:
        for idx, old_res in enumerate(job["results"]):
            if old_res.row == new_res.row:
                job["results"][idx] = new_res
                break

    # Re-evaluate activation
    batch_activated = False
    if job["failed"] == 0:
        async with httpx.AsyncClient() as client:
            resp = await client.patch(f"{EXTERNAL_API_BASE_URL}/hospitals/batch/{batch_id}/activate")
            if resp.status_code == 200:
                batch_activated = True

    return BulkProcessResponse(
        batch_id=batch_id,
        total_hospitals=job["total"],
        processed_hospitals=job["processed"],
        failed_hospitals=job["failed"],
        batch_activated=batch_activated,
        hospitals=job["results"]
    )

@app.websocket("/hospitals/bulk/progress/{batch_id}")
async def websocket_endpoint(websocket: WebSocket, batch_id: str):
    """Bonus: Real-time progress updates via WebSocket."""
    await manager.connect(websocket, batch_id)
    try:
        while True:
            await websocket.receive_text() # Keeps connection open
    except WebSocketDisconnect:
        manager.disconnect(websocket, batch_id)