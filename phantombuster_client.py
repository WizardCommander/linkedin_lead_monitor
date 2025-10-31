"""
PhantomBuster API client for LinkedIn content search automation

Handles agent launches, status polling, and output retrieval.
"""

import os
import time
import requests
from typing import Optional, Dict, Any, List


class PhantomBusterClient:
    """Client for interacting with PhantomBuster API v1"""

    BASE_URL = "https://api.phantombuster.com/api/v1"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize PhantomBuster client

        Args:
            api_key: PhantomBuster API key. If None, reads from PHANTOMBUSTER_API_KEY env var
        """
        self.api_key = api_key or os.getenv("PHANTOMBUSTER_API_KEY")
        if not self.api_key:
            raise ValueError("PhantomBuster API key required")

        self.session = requests.Session()
        self.session.headers.update({"X-Phantombuster-Key-1": self.api_key})

    def launch_agent(
        self,
        agent_id: str,
        search_url: str,
        session_cookie: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Launch a PhantomBuster agent with a LinkedIn search URL

        Args:
            agent_id: PhantomBuster agent ID
            search_url: LinkedIn search URL to scrape
            session_cookie: LinkedIn li_at session cookie. If None, reads from env

        Returns:
            Dict with keys:
                - success: bool - True if launch succeeded
                - container_id: str - Container ID if launched
                - message: str - Status message (e.g. "already retrieved")
                - cached: bool - True if using cached results

        Raises:
            Exception: If agent launch fails unexpectedly
        """
        linkedin_cookie = session_cookie or os.getenv("LINKEDIN_SESSION_COOKIE")
        if not linkedin_cookie:
            raise ValueError("LinkedIn session cookie (li_at) required")

        endpoint = f"{self.BASE_URL}/agent/{agent_id}/launch"

        # Send both search URL and sessionCookie
        payload = {
            "argument": {
                "search": search_url,
                "sessionCookie": linkedin_cookie,
            }
        }

        response = self.session.post(endpoint, json=payload)

        # Handle success case
        if response.status_code == 200:
            data = response.json()

            # PhantomBuster API returns: {"status": "success", "data": {"containerId": "..."}}
            if "data" in data and isinstance(data["data"], dict):
                container_id = data["data"].get("containerId")
            else:
                # Fallback for direct format: {"containerId": "..."}
                container_id = data.get("containerId")

            if not container_id:
                raise Exception(f"No container ID in response: {data}")

            return {
                "success": True,
                "container_id": container_id,
                "message": "Agent launched successfully",
                "cached": False,
            }

        # Handle "already retrieved" case (PhantomBuster returns success but with message)
        # This happens when PB has cached results for this search
        try:
            data = response.json()
            message = data.get("message", "")

            if "already retrieved" in message.lower():
                return {
                    "success": True,
                    "container_id": None,
                    "message": message,
                    "cached": True,
                }
        except:
            pass

        # For other errors, raise
        response.raise_for_status()
        raise Exception(f"Unexpected response from PhantomBuster: {response.text}")

    def get_agent_status(self, agent_id: str, container_id: str) -> Dict[str, Any]:
        """
        Get status of a running agent container

        Args:
            agent_id: PhantomBuster agent ID
            container_id: Container ID from launch_agent()

        Returns:
            Status dict with keys: lastEndStatus, exitCode, etc.
        """
        endpoint = f"{self.BASE_URL}/agent/{agent_id}/containers"
        response = self.session.get(endpoint)
        response.raise_for_status()

        data = response.json()

        # PhantomBuster returns: {"status": "success", "data": [containers...]}
        containers = data.get("data", [])

        # Try both string and int comparison since API might return either format
        for container in containers:
            cid = container.get("id")
            if str(cid) == str(container_id):
                return container

        # Debug: show what containers we found
        container_ids = [str(c.get("id")) for c in containers[:5]]  # Show first 5
        raise Exception(
            f"Container {container_id} not found in agent {agent_id}. "
            f"Found {len(containers)} containers. Recent IDs: {container_ids}"
        )

    def wait_for_completion(
        self,
        agent_id: str,
        container_id: str,
        poll_interval: int = 30,
        timeout: int = 380,
    ) -> Dict[str, Any]:
        """
        Poll agent status until completion or timeout

        Args:
            agent_id: PhantomBuster agent ID
            container_id: Container ID from launch_agent()
            poll_interval: Seconds between status checks (default 30)
            timeout: Max seconds to wait (default 380 = 6.3 minutes)

        Returns:
            Final status dict

        Raises:
            TimeoutError: If agent doesn't complete within timeout
            Exception: If agent fails
        """
        start_time = time.time()

        # Initial delay to let container appear in API (race condition fix)
        time.sleep(2)

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                raise TimeoutError(
                    f"Agent {agent_id} did not complete within {timeout}s"
                )

            try:
                container = self.get_agent_status(agent_id, container_id)
            except Exception as e:
                # If container not found yet, wait and retry
                if "not found" in str(e):
                    print(f"    Container not found yet, retrying in 5s...")
                    time.sleep(5)
                    continue
                raise

            # Container status fields: lastEndStatus (success/error), endDate (timestamp when done)
            last_status = container.get("lastEndStatus")
            end_date = container.get("endDate")

            if end_date:
                # Container has finished
                print(f"    Status: finished ({last_status})")
                if last_status == "success":
                    return container
                else:
                    error_msg = container.get("exitMessage", last_status or "Unknown error")
                    raise Exception(f"Agent {agent_id} failed: {error_msg}")
            else:
                # Still running
                print(f"    Status: running...")

            time.sleep(poll_interval)

    def fetch_output(self, agent_id: str, container_id: str) -> List[Dict[str, Any]]:
        """
        Fetch scraped data from completed agent

        Uses PhantomBuster API v2 to get result object which contains S3 URLs,
        then fetches the actual scraped data from the S3 JSON file.

        Args:
            agent_id: PhantomBuster agent ID
            container_id: Container ID from launch_agent()

        Returns:
            List of scraped post data dicts

        Raises:
            ValueError: If response format is unexpected or no results found
        """
        # Get result object which contains S3 URLs
        endpoint = "https://api.phantombuster.com/api/v2/containers/fetch-result-object"

        print(f"    Fetching result object (container: {container_id})...")

        response = self.session.get(endpoint, params={"id": container_id})
        response.raise_for_status()

        data = response.json()

        # Response format: {"resultObject": '{"csvURL":"...","jsonUrl":"..."}'}
        if not isinstance(data, dict) or "resultObject" not in data:
            raise ValueError(f"No resultObject in response: {data}")

        result_obj = data["resultObject"]

        if not result_obj:
            print(f"    Warning: resultObject is null (no data scraped)")
            return []

        # Parse the result object JSON string
        import json as json_module
        if isinstance(result_obj, str):
            result_obj = json_module.loads(result_obj)

        # Extract JSON URL from result object
        if not isinstance(result_obj, dict) or "jsonUrl" not in result_obj:
            raise ValueError(f"No jsonUrl in resultObject: {result_obj}")

        json_url = result_obj["jsonUrl"]
        print(f"    Fetching scraped data from S3...")

        # Fetch the actual scraped LinkedIn posts from S3
        s3_response = requests.get(json_url)
        s3_response.raise_for_status()

        posts = s3_response.json()

        if isinstance(posts, list):
            return posts
        else:
            raise ValueError(
                f"Unexpected S3 data format: {type(posts).__name__}. "
                f"Expected list, got: {str(posts)[:200]}"
            )

    def get_all_containers(self, agent_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get all containers for an agent (historical execution data)

        Args:
            agent_id: PhantomBuster agent ID
            limit: Maximum number of containers to retrieve (default 50)

        Returns:
            List of container dicts with keys: id, lastEndStatus, endDate, etc.
            Sorted by most recent first
        """
        endpoint = f"{self.BASE_URL}/agent/{agent_id}/containers"
        response = self.session.get(endpoint)
        response.raise_for_status()

        data = response.json()

        # PhantomBuster returns: {"status": "success", "data": [containers...]}
        containers = data.get("data", [])

        # Sort by most recent first (containers with endDate)
        containers_with_date = [c for c in containers if c.get("endDate")]
        containers_with_date.sort(key=lambda c: c.get("endDate", ""), reverse=True)

        return containers_with_date[:limit]

    def fetch_output_by_container_id(self, container_id: str) -> List[Dict[str, Any]]:
        """
        Fetch scraped data from any container ID (historical or recent)

        This is useful for fetching cached results without needing the agent_id.

        Args:
            container_id: Container ID to fetch results from

        Returns:
            List of scraped post data dicts

        Raises:
            ValueError: If response format is unexpected or no results found
        """
        # Get result object which contains S3 URLs
        endpoint = "https://api.phantombuster.com/api/v2/containers/fetch-result-object"

        print(f"    Fetching result object (container: {container_id})...")

        response = self.session.get(endpoint, params={"id": container_id})
        response.raise_for_status()

        data = response.json()

        # Response format: {"resultObject": '{"csvURL":"...","jsonUrl":"..."}'}
        if not isinstance(data, dict) or "resultObject" not in data:
            raise ValueError(f"No resultObject in response: {data}")

        result_obj = data["resultObject"]

        if not result_obj:
            print(f"    Warning: resultObject is null (no data scraped)")
            return []

        # Parse the result object JSON string
        import json as json_module

        if isinstance(result_obj, str):
            result_obj = json_module.loads(result_obj)

        # Extract JSON URL from result object
        if not isinstance(result_obj, dict) or "jsonUrl" not in result_obj:
            raise ValueError(f"No jsonUrl in resultObject: {result_obj}")

        json_url = result_obj["jsonUrl"]
        print(f"    Fetching scraped data from S3...")

        # Fetch the actual scraped LinkedIn posts from S3
        s3_response = requests.get(json_url)
        s3_response.raise_for_status()

        posts = s3_response.json()

        if isinstance(posts, list):
            return posts
        else:
            raise ValueError(
                f"Unexpected S3 data format: {type(posts).__name__}. "
                f"Expected list, got: {str(posts)[:200]}"
            )
