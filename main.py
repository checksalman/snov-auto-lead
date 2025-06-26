from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import asyncio

app = FastAPI()

# Your Snov.io credentials
CLIENT_ID = "6738400dc1c082262546a8f7a8b76601"
CLIENT_SECRET = "cd413b651eda5fb2ee2bc82036313713"
TARGET_LIST_ID = "31583921"

ACCESS_TOKEN = None

class CompanyRequest(BaseModel):
    domain: str

async def get_access_token():
    global ACCESS_TOKEN
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.snov.io/v1/oauth/access_token",
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET
            }
        )
        if resp.status_code != 200:
            print("Auth failed:", resp.text)
            raise HTTPException(status_code=500, detail="Snov.io auth failed")
        ACCESS_TOKEN = resp.json().get("access_token")
        if not ACCESS_TOKEN:
            print("No access token in response:", resp.text)
            raise HTTPException(status_code=500, detail="Access token missing")
        print("✅ Access token received.")
        return ACCESS_TOKEN

async def start_domain_search(domain):
    print(f"🔹 Starting domain search for: {domain}")
    await get_access_token()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.snov.io/v2/domain-search/start",
            json={"domain": domain},
            headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
        )
        if resp.status_code not in [200, 202]:
            print(f"Domain search request failed [{resp.status_code}]:", resp.text)
            raise HTTPException(status_code=500, detail="Domain search request failed")

        task_hash = resp.json().get("meta", {}).get("task_hash")
        if not task_hash:
            print("No task_hash received:", resp.text)
            raise HTTPException(status_code=500, detail="No task_hash received")

        print(f"✅ Domain search task hash: {task_hash}")
        return task_hash

async def poll_domain_search_result(task_hash):
    async with httpx.AsyncClient() as client:
        for attempt in range(20):
            resp = await client.get(
                f"https://api.snov.io/v2/domain-search/result/{task_hash}",
                headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
            )
            if resp.status_code != 200:
                print("Polling domain search failed:", resp.text)
                raise HTTPException(status_code=500, detail="Polling domain search failed")
            
            data = resp.json()
            status = data.get("status")
            print(f"🔄 Domain search poll {attempt+1}/20: status={status}")
            meta = data.get("meta", {})
            info = data.get("data", {})
            print(f"ℹ️ Company: {info.get('company_name')}, Size: {info.get('size')}, Prospects: {meta.get('prospects_count')}, Emails: {meta.get('emails_count')}")
            
            if status == "completed":
                print("✅ Domain search completed.")
                return data
            await asyncio.sleep(5)
    raise HTTPException(status_code=504, detail="Domain search polling timed out")

async def start_prospect_search(prospects_url):
    print("🔹 Starting prospect search...")
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            prospects_url,
            headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
        )
        if resp.status_code not in [200, 202]:
            print(f"Prospect search start failed [{resp.status_code}]:", resp.text)
            raise HTTPException(status_code=500, detail="Prospect search start failed")
        
        task_hash = resp.json().get("meta", {}).get("task_hash")
        if not task_hash:
            print("No task_hash for prospect search:", resp.text)
            raise HTTPException(status_code=500, detail="No task_hash for prospect search")
        
        print(f"✅ Prospect search task hash: {task_hash}")
        return task_hash

async def poll_prospect_result(task_hash):
    async with httpx.AsyncClient() as client:
        for attempt in range(20):
            resp = await client.get(
                f"https://api.snov.io/v2/domain-search/prospects/result/{task_hash}",
                headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
            )
            if resp.status_code != 200:
                print("Polling prospect result failed:", resp.text)
                raise HTTPException(status_code=500, detail="Polling prospect result failed")
            
            data = resp.json()
            status = data.get("status")
            print(f"🔄 Prospect search poll {attempt+1}/20: status={status}")
            
            if status == "completed":  # FIXED: status 'completed' indicates ready
                prospects = data.get("prospects", [])
                print(f"✅ Prospect search completed. Prospects retrieved: {len(prospects)}")
                return prospects
            
            await asyncio.sleep(5)
    raise HTTPException(status_code=504, detail="Prospect polling timed out")

async def add_prospect(prospect):
    print(f"➡️ Adding prospect: {prospect.get('email')} | Position: {prospect.get('position')}")
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.snov.io/v1/prospect",
            json={
                "email": prospect.get("email"),
                "firstName": prospect.get("first_name", ""),
                "lastName": prospect.get("last_name", ""),
                "customFields": [{"name": "job_position", "value": prospect.get("position", "")}],
                "listId": TARGET_LIST_ID
            },
            headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
        )
        if resp.status_code != 200:
            print(f"❌ Failed to add: {prospect.get('email')}, Reason: {resp.text}")

@app.post("/find-buyers")
async def find_buyers(req: CompanyRequest):
    # Phase 1: domain search
    domain_task = await start_domain_search(req.domain)
    domain_data = await poll_domain_search_result(domain_task)
    
    prospects_url = domain_data.get("links", {}).get("prospects")
    if not prospects_url:
        raise HTTPException(status_code=500, detail="No prospects URL returned from domain search")
    
    # Phase 2: start and poll prospects
    prospect_task = await start_prospect_search(prospects_url)
    prospects = await poll_prospect_result(prospect_task)

    print(f"🔹 Filtering for positions matching ['buyer', 'purchase', 'purchasing agent']")
    filtered = [
        p for p in prospects
        if p.get("position") and any(
            kw in p["position"].lower() for kw in ["buyer", "purchase", "purchasing agent"]
        )
    ]
    print(f"✅ Filtered prospects count: {len(filtered)}")

    # Phase 3: add prospects
    for p in filtered:
        await add_prospect(p)

    return {"checked": len(prospects), "added": len(filtered)}
