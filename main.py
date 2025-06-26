from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import asyncio

app = FastAPI()

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
            print("❌ Auth failed:", resp.text)
            raise HTTPException(status_code=500, detail="Snov.io auth failed")
        ACCESS_TOKEN = resp.json().get("access_token")
        if not ACCESS_TOKEN:
            print("❌ No access token in response:", resp.text)
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
            print("❌ Domain search failed:", resp.text)
            raise HTTPException(status_code=500, detail="Domain search failed")
        task_hash = resp.json().get("meta", {}).get("task_hash")
        print(f"✅ Domain search task hash: {task_hash}")
        return task_hash

async def poll_domain_result(task_hash):
    async with httpx.AsyncClient() as client:
        for attempt in range(20):
            resp = await client.get(
                f"https://api.snov.io/v2/domain-search/result/{task_hash}",
                headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
            )
            if resp.status_code != 200:
                raise HTTPException(status_code=500, detail="Polling domain result failed")
            data = resp.json()
            print(f"🔄 Domain poll {attempt+1}: status={data.get('status')}")
            if data.get("status") == "completed":
                print(f"✅ Domain search completed: Meta={data.get('meta')}")
                print(f"🌐 Links: {data.get('links')}")
                return data
            await asyncio.sleep(5)
    raise HTTPException(status_code=504, detail="Domain polling timed out")

async def start_prospect_search(prospect_url):
    print(f"🔹 Starting prospect search at URL: {prospect_url}")
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            prospect_url,
            headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
        )
        if resp.status_code not in [200, 202]:
            print("❌ Prospect search failed:", resp.text)
            return None
        task_hash = resp.json().get("meta", {}).get("task_hash")
        print(f"✅ Prospect search task hash: {task_hash}")
        return task_hash

async def poll_prospect_result(task_hash):
    if not task_hash:
        print("⚠️ No task hash provided for prospect polling.")
        return []
    async with httpx.AsyncClient() as client:
        for attempt in range(20):
            resp = await client.get(
                f"https://api.snov.io/v2/domain-search/prospects/result/{task_hash}",
                headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
            )
            if resp.status_code != 200:
                print("❌ Polling prospect result failed:", resp.text)
                return []
            data = resp.json()
            print(f"🔄 Prospect poll {attempt+1}: status={data.get('status')}")
            if data.get("status") == "completed":
                prospects = data.get("prospects", [])
                print(f"✅ Prospects retrieved: {len(prospects)}")
                return prospects
            await asyncio.sleep(5)
    print("⚠️ Prospect polling timed out.")
    return []

async def fetch_emails_for_prospect(search_email_url):
    if not search_email_url:
        print("⚠️ No search email URL provided.")
        return None
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            search_email_url,
            headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
        )
        if resp.status_code not in [200, 202]:
            print("❌ Email search start failed:", resp.text)
            return None
        task_hash = resp.json().get("task_hash")
        for attempt in range(10):
            result_resp = await client.get(
                f"https://api.snov.io/v2/domain-search/prospects/search-emails/result/{task_hash}",
                headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
            )
            if result_resp.status_code != 200:
                print("❌ Email search result failed:", result_resp.text)
                return None
            data = result_resp.json()
            if data.get("status") == "completed":
                emails = data.get("emails", [])
                if emails:
                    print(f"✅ Email found: {emails[0].get('email')}")
                    return emails[0].get("email")
                else:
                    print("⚠️ No email found.")
                    return None
            await asyncio.sleep(3)
        print("⚠️ Email search timed out.")
        return None

async def add_prospect_to_list(prospect, email):
    print(f"➡️ Adding to list: {email} | Position: {prospect.get('position')}")
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.snov.io/v1/prospect",
            json={
                "email": email,
                "firstName": prospect.get("first_name", ""),
                "lastName": prospect.get("last_name", ""),
                "customFields": [{"name": "job_position", "value": prospect.get("position", "")}],
                "listId": TARGET_LIST_ID
            },
            headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}
        )
        if resp.status_code != 200:
            print(f"❌ Add prospect failed: {resp.text}")

@app.post("/find-buyers")
async def find_buyers(req: CompanyRequest):
    domain_task = await start_domain_search(req.domain)
    domain_data = await poll_domain_result(domain_task)

    prospects_url = domain_data.get("links", {}).get("prospects")
    if not prospects_url:
        print("⚠️ No prospects URL found — exiting cleanly.")
        return {"checked": 0, "added": 0}

    prospect_task = await start_prospect_search(prospects_url)
    prospects = await poll_prospect_result(prospect_task)

    print("🔹 Filtering for buyer-related positions...")
    filtered = [
        p for p in prospects
        if p.get("position") and any(
            kw in p["position"].lower() for kw in ["buyer", "purchase", "purchasing agent"]
        )
    ]
    print(f"✅ Filtered prospects: {len(filtered)}")

    added_count = 0
    for p in filtered:
        email = await fetch_emails_for_prospect(p.get("search_emails_start"))
        if email:
            await add_prospect_to_list(p, email)
            added_count += 1

    return {"checked": len(prospects), "added": added_count}
