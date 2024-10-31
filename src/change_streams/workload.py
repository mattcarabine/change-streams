import asyncio
import random
import httpx
import json
from typing import List, Dict
import time
from datetime import datetime
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WorkloadGenerator:
    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        collections: List[str] = ["users", "orders", "products"],
        rate: float = 1.0  # operations per second
    ):
        self.base_url = base_url
        self.collections = collections
        self.rate = rate
        self.client = httpx.AsyncClient()
        
        # Sample data for generating realistic content
        self.names = ["John", "Jane", "Bob", "Alice", "Charlie"]
        self.cities = ["New York", "London", "Paris", "Tokyo", "Berlin"]
        self.statuses = ["active", "pending", "inactive", "deleted"]
        self.products = ["Laptop", "Phone", "Tablet", "Watch", "Headphones"]
        
        # Track existing keys for updates/deletes
        self.existing_keys: Dict[str, List[str]] = {c: [] for c in collections}

    def generate_value(self, collection: str) -> Dict:
        """Generate realistic data based on collection type."""
        if collection == "users":
            return {
                "name": random.choice(self.names),
                "age": random.randint(18, 80),
                "city": random.choice(self.cities),
                "status": random.choice(self.statuses),
                "email": f"{random.choice(self.names).lower()}@example.com",
                "created_at": datetime.now().isoformat()
            }
        elif collection == "orders":
            return {
                "user_id": f"user:{random.randint(1, 100)}",
                "product": random.choice(self.products),
                "quantity": random.randint(1, 5),
                "status": random.choice(["placed", "shipped", "delivered"]),
                "total": round(random.uniform(10, 1000), 2),
                "created_at": datetime.now().isoformat()
            }
        elif collection == "products":
            return {
                "name": random.choice(self.products),
                "price": round(random.uniform(10, 1000), 2),
                "stock": random.randint(0, 100),
                "status": random.choice(["in_stock", "low_stock", "out_of_stock"]),
                "category": random.choice(["electronics", "accessories"]),
                "updated_at": datetime.now().isoformat()
            }
        return {"value": "default"}

    async def create_document(self, collection: str) -> None:
        """Create a new document."""
        key = f"{collection[:-1]}:{len(self.existing_keys[collection]) + 1}"
        value = self.generate_value(collection)
        
        try:
            response = await self.client.put(
                f"{self.base_url}/{collection}/documents/{key}",
                json={"value": value}
            )
            if response.status_code == 200:
                self.existing_keys[collection].append(key)
                logger.info(f"Created {collection}/{key}")
        except Exception as e:
            logger.error(f"Error creating document: {e}")

    async def update_document(self, collection: str) -> None:
        """Update an existing document."""
        if not self.existing_keys[collection]:
            return
        
        key = random.choice(self.existing_keys[collection])
        value = self.generate_value(collection)
        
        try:
            response = await self.client.put(
                f"{self.base_url}/{collection}/documents/{key}",
                json={"value": value}
            )
            if response.status_code == 200:
                logger.info(f"Updated {collection}/{key}")
        except Exception as e:
            logger.error(f"Error updating document: {e}")

    async def delete_document(self, collection: str) -> None:
        """Delete an existing document."""
        if not self.existing_keys[collection]:
            return
        
        key = random.choice(self.existing_keys[collection])
        
        try:
            response = await self.client.delete(
                f"{self.base_url}/{collection}/documents/{key}"
            )
            if response.status_code == 200:
                self.existing_keys[collection].remove(key)
                logger.info(f"Deleted {collection}/{key}")
        except Exception as e:
            logger.error(f"Error deleting document: {e}")

    async def query_documents(self, collection: str) -> None:
        """Perform random queries."""
        queries = [
            "value.status = 'active'",
            "value.age > 25",
            f"value.city = '{random.choice(self.cities)}'",
            "value.status IN ('active', 'pending')",
            "value.email IS NOT NULL"
        ]
        
        try:
            response = await self.client.get(
                f"{self.base_url}/{collection}/documents",
                params={"where": random.choice(queries)}
            )
            if response.status_code == 200:
                logger.info(f"Queried {collection}")
        except Exception as e:
            logger.error(f"Error querying documents: {e}")

    async def run_operation(self) -> None:
        """Run a random operation."""
        collection = random.choice(self.collections)
        operation = random.choices(
            ["create", "update", "delete", "query"],
            weights=[0.4, 0.3, 0.1, 0.2]
        )[0]
        
        if operation == "create":
            await self.create_document(collection)
        elif operation == "update":
            await self.update_document(collection)
        elif operation == "delete":
            await self.delete_document(collection)
        else:
            await self.query_documents(collection)

    async def run(self, duration: int = 60):
        """
        Run the workload generator for a specified duration.
        
        Args:
            duration: Number of seconds to run
        """
        start_time = time.time()
        operations = 0
        
        while time.time() - start_time < duration:
            await self.run_operation()
            operations += 1
            
            # Sleep to maintain desired rate
            await asyncio.sleep(1 / self.rate)
        
        await self.client.aclose()
        logger.info(f"Completed {operations} operations in {duration} seconds")

async def main():
    parser = argparse.ArgumentParser(description="API Workload Generator")
    parser.add_argument("--rate", type=float, default=1.0, help="Operations per second")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    args = parser.parse_args()
    
    generator = WorkloadGenerator(rate=args.rate)
    await generator.run(duration=args.duration)

if __name__ == "__main__":
    asyncio.run(main())
