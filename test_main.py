import pytest
from fastapi.testclient import TestClient
from main import app, MAX_CSV_ROWS

client = TestClient(app)

def test_upload_non_csv_file():
    response = client.post(
        "/hospitals/bulk",
        files={"file": ("test.txt", b"dummy content", "text/plain")}
    )
    assert response.status_code == 400
    assert "Only CSV files" in response.json()["detail"]

def test_exceed_max_rows():
    # Dummy csv
    header = "name,address,phone\n"
    rows = "".join([f"Hospital {i},Address {i},555-0000\n" for i in range(MAX_CSV_ROWS + 1)])
    csv_content = header + rows
    
    response = client.post(
        "/hospitals/bulk",
        files={"file": ("test.csv", csv_content.encode('utf-8'), "text/csv")}
    )
    assert response.status_code == 400
    assert "exceeds limit" in response.json()["detail"]

def test_missing_required_columns():
    csv_content = "name,phone\nHospital A,555-1234\n"
    response = client.post(
        "/hospitals/bulk",
        files={"file": ("test.csv", csv_content.encode('utf-8'), "text/csv")}
    )
    assert response.status_code == 400
    assert "must contain 'name' and 'address'" in response.json()["detail"]