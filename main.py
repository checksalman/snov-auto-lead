import logging
import asyncio
import httpx
import re
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel

# Snov.io API credentials and target list ID
# IMPORTANT: In a production environment, consider using environment variables for sensitive data.
CLIENT_ID = "6738400dc1c082262546a8f7a8b76601"
CLIENT_SECRET = "cd413b651eda5fb2ee2bc82036313713"
TARGET_LIST_ID = "31583921"

# Keywords for prospect positions (case-insensitive and with word boundaries for accuracy)
TARGET_KEYWORDS = [
    "buyer", "purchase", "ceo", "managing director", "procure", "procurement"
]

# Configure logging for detailed output during execution
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize FastAPI application
app = FastAPI(
    title="Snov.io Prospect Automation",
    description="API to find and add prospects from a given domain to a Snov.io list.",
    version="1.0.0"
)

# Global dictionary to store the Snov.io access token and its expiry time.
# This helps in caching the token and avoiding re-authentication on every request.
snov_access_token = {
    "token": None,
    "expires_at": 0 # Unix timestamp
}

# Base URLs for Snov.io API endpoints
SNOV_API_BASE_URL = "https://api.snov.io/v1"
OAUTH_TOKEN_URL = "https://api.snov.io/v1/oauth/access_token"

class DomainRequest(BaseModel):
    """
    Pydantic model for validating the incoming request body.
    Ensures that the 'domain' field is provided.
    """
    domain: str

async def get_snov_access_token():
    """
    Obtains or refreshes the Snov.io API access token.
    Tokens are cached and refreshed if they are nearing expiry.
    """
    # Use event loop time for consistency within the async application
    current_time = asyncio.get_event_loop().time()

    # Check if the cached token is still valid (with a buffer of 60 seconds)
    if snov_access_token["token"] and snov_access_token["expires_at"] > current_time + 60:
        logger.info("Using cached Snov.io access token.")
        return snov_access_token["token"]

    logger.info("Attempting to obtain a new Snov.io access token...")
    try:
        async with httpx.AsyncClient() as client:
            # Make a POST request to the OAuth token endpoint
            response = await client.post(
                OAUTH_TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET
                }
            )
            response.raise_for_status() # Raise an exception for 4xx/5xx status codes
            token_data = response.json()

            if "access_token" in token_data:
                # Store the new token and calculate its expiry time
                snov_access_token["token"] = token_data["access_token"]
                # Reduce expiry by 120 seconds to pre-emptively refresh
                snov_access_token["expires_at"] = current_time + token_data.get("expires_in", 3600) - 120
                logger.info("Successfully obtained new Snov.io access token.")
                return snov_access_token["token"]
            else:
                # Log error if access_token is not in the response
                logger.error(f"Failed to get access token: {token_data.get('message', 'Unknown error')}")
                raise HTTPException(status_code=500, detail="Failed to obtain Snov.io access token.")
    except httpx.HTTPStatusError as e:
        # Handle HTTP status errors (e.g., 401, 403, 500 from Snov.io)
        logger.error(f"HTTP error getting Snov.io token: {e.response.status_code} - {e.response.text}")
        raise HTTPException(status_code=500, detail=f"Snov.io token HTTP error: {e.response.text}")
    except httpx.RequestError as e:
        # Handle network-related errors (e.g., DNS resolution, connection refused)
        logger.error(f"Network error getting Snov.io token: {e}")
        raise HTTPException(status_code=500, detail=f"Snov.io token network error: {e}")
    except Exception as e:
        # Catch any other unexpected errors during token retrieval
        logger.error(f"Unexpected error getting Snov.io token: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error obtaining Snov.io access token: {e}")

async def call_snov_api(endpoint: str, data: dict):
    """
    Helper function to make authenticated POST requests to Snov.io API endpoints.
    Automatically adds the access token and handles common API error responses.
    """
    access_token = await get_snov_access_token()
    data["access_token"] = access_token # Add the access token to the request payload
    full_url = f"{SNOV_API_BASE_URL}/{endpoint}"
    logger.debug(f"Calling Snov.io API: {full_url} with data: {data}")

    try:
        # Use httpx.AsyncClient with a timeout to prevent indefinite waits
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(full_url, data=data)
            response.raise_for_status() # Raise an exception for 4xx/5xx responses

            result = response.json()

            # Snov.io API typically indicates errors with "success": false
            if isinstance(result, dict) and result.get("success") is False:
                error_message = result.get("message", "Unknown Snov.io API error.")
                logger.error(f"Snov.io API call failed for {endpoint}: {error_message}")
                raise HTTPException(status_code=400, detail=f"Snov.io API error: {error_message}")
            return result
    except httpx.HTTPStatusError as e:
        # Handle HTTP status errors from Snov.io API calls
        logger.error(f"HTTP error during Snov.io API call to {endpoint}: {e.response.status_code} - {e.response.text}")
        raise HTTPException(status_code=e.response.status_code, detail=f"Snov.io API HTTP error: {e.response.text}")
    except httpx.RequestError as e:
        # Handle network errors during API calls
        logger.error(f"Network error during Snov.io API call to {endpoint}: {e}")
        raise HTTPException(status_code=500, detail=f"Snov.io API network error: {e}")
    except Exception as e:
        # Catch any other unexpected errors
        logger.error(f"Unexpected error during Snov.io API call to {endpoint}: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error during Snov.io API call: {e}")

@app.post("/process-domain-prospects")
async def process_domain_prospects(domain_request: DomainRequest):
    """
    API endpoint to process a given domain, find relevant prospects,
    and add them to a predefined Snov.io list.

    Args:
        domain_request (DomainRequest): Pydantic model containing the domain URL.

    Returns:
        dict: A summary of the operation, including the number of prospects checked
              and the number of prospects added to the list.
    """
    domain = domain_request.domain
    logger.info(f"Received request to process domain: {domain}")

    checked_prospects_count = 0
    added_prospects_count = 0

    try:
        # 1. Start and Poll domain search via Snov API's 'domain-search' endpoint.
        # Snov.io's 'domain-search' API might return 'processing' status,
        # requiring repeated calls until 'done'.
        logger.info(f"Initiating domain search for {domain}...")
        domain_search_result = {}
        status = "processing"
        polling_attempts = 0
        MAX_POLLING_ATTEMPTS = 30 # Maximum number of times to poll
        POLLING_DELAY = 5 # Seconds to wait between polling attempts

        while status == "processing" and polling_attempts < MAX_POLLING_ATTEMPTS:
            domain_search_result = await call_snov_api(
                "domain-search",
                {"domain": domain}
            )
            status = domain_search_result.get("status", "error")
            logger.info(f"Domain search status for {domain}: {status} (Attempt {polling_attempts + 1}/{MAX_POLLING_ATTEMPTS})")

            if status == "processing":
                polling_attempts += 1
                logger.info(f"Waiting {POLLING_DELAY} seconds before next poll...")
                await asyncio.sleep(POLLING_DELAY)
            elif status == "error":
                # If Snov.io returns an 'error' status for the search
                error_message = domain_search_result.get("message", "Unknown error during domain search.")
                raise HTTPException(status_code=500, detail=f"Snov.io domain search returned an error for {domain}: {error_message}")
            elif status == "done":
                # Search is complete, break the loop
                break
        
        # If the loop finishes but status is not 'done', it means polling timed out
        if status != "done":
            logger.error(f"Domain search for {domain} did not complete within {MAX_POLLING_ATTEMPTS} attempts.")
            raise HTTPException(status_code=504, detail=f"Snov.io domain search timed out for {domain}. Please try again later.")

        # Extract prospects from the search result. 'emails' contains the prospect data.
        prospects = domain_search_result.get("emails", [])
        logger.info(f"Retrieved {len(prospects)} prospects for domain {domain}.")
        checked_prospects_count = len(prospects)

        # 2. Iterate through each retrieved prospect and filter based on keywords.
        for prospect in prospects:
            position = prospect.get("position", "")
            email = prospect.get("email")
            first_name = prospect.get("firstName", "")
            last_name = prospect.get("lastName", "")
            # Use domain as company name if not explicitly provided by Snov.io for the prospect
            company_name = prospect.get("companyName", domain) 

            if not email:
                logger.warning(f"Skipping prospect due to missing email in Snov.io data: {prospect}")
                continue

            # Check if the prospect's position contains any of the target keywords.
            # Using re.search with '\b' for whole word matching and re.IGNORECASE for case-insensitivity.
            is_match = any(re.search(r'\b' + re.escape(keyword) + r'\b', position, re.IGNORECASE) for keyword in TARGET_KEYWORDS)

            if is_match:
                logger.info(f"Matching prospect found: {email} (Position: '{position}'). Attempting to add to list {TARGET_LIST_ID}...")
                # 3. Add the matching prospect to the predefined Snov.io list.
                try:
                    add_result = await call_snov_api(
                        "add-prospect-to-list",
                        {
                            "email": email,
                            "firstName": first_name,
                            "lastName": last_name,
                            "list_id": TARGET_LIST_ID,
                            "position": position,
                            "companyName": company_name
                        }
                    )
                    if add_result.get("success"):
                        added_prospects_count += 1
                        logger.info(f"Successfully added {email} to list {TARGET_LIST_ID}.")
                    else:
                        # Log if Snov.io API explicitly indicates failure to add
                        logger.warning(f"Failed to add {email} to list: {add_result.get('message', 'Unknown error')}")
                except HTTPException as e:
                    logger.error(f"API Error while adding prospect {email} to list: {e.detail}")
                except Exception as e:
                    logger.error(f"Unexpected error while adding prospect {email} to list: {e}")
            else:
                logger.info(f"Prospect {email} with position '{position}' does not match target keywords. Skipping.")

        logger.info(f"Finished processing domain {domain}. Summary: Checked Prospects = {checked_prospects_count}, Added Prospects = {added_prospects_count}")
        
        # Return the final summary of the operation
        return {
            "checked": checked_prospects_count,
            "added": added_prospects_count
        }

    except HTTPException as e:
        # Re-raise HTTPException to be handled by FastAPI's error handlers
        logger.error(f"API Error processing domain {domain}: {e.detail}")
        raise e
    except Exception as e:
        # Catch any unexpected critical errors and log them with traceback
        logger.critical(f"An unhandled exception occurred while processing domain {domain}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

