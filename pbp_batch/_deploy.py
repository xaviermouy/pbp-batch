from pbp_batch.core import submit_job
from prefect.filesystems import LocalFileSystem
from prefect.client.orchestration import get_client
from prefect.exceptions import ObjectAlreadyExists
import asyncio

WORK_POOL_NAME = "Xavier-work-pool"  # Ensure this exists

async def storage_exists(name: str) -> bool:
    """Check if a storage block already exists in Prefect."""
    async with get_client() as client:
        blocks = await client.read_block_documents()
        return any(block.name == name for block in blocks)

def create_deployment():
    """Create and apply a Prefect deployment with storage."""

    storage = LocalFileSystem(basepath="./pbp_batch")
    storage_name = "local-storage"

    # Check if the storage block already exists
    if not asyncio.run(storage_exists(storage_name)):
        storage.save(storage_name)  # Save the storage configuration
    else:
        storage.save(storage_name, overwrite=True)  # Overwrite if it exists

    # Deploy the flow
    deployment = submit_job.deploy(
        name="pbp_batch_deployment",
        work_pool_name=WORK_POOL_NAME,
        storage=storage
    )

    print(f"Deployment '{deployment.name}' applied successfully.")

if __name__ == "__main__":
    create_deployment()