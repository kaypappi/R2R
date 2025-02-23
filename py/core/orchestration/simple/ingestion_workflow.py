from uuid import UUID
from typing import List
from log import logger
from core.models.graph_construction_status import GraphConstructionStatus

async def ingest_document(document_info, collection_ids: List[str] = None):
    if not collection_ids:
        # TODO: Move logic onto the `management service`
        collection_id = generate_default_user_collection_id(
            document_info.owner_id
        )
        collection_id_uuid = UUID(str(collection_id))
        await service.providers.database.collections_handler.assign_document_to_collection_relational(
            document_id=document_info.id,
            collection_id=collection_id_uuid,
        )
        await service.providers.database.collections_handler.update_document_count_recursively(
            collection_id_uuid, increment=True, change_amount=1
        )
        await service.providers.database.chunks_handler.assign_document_chunks_to_collection(
            document_id=document_info.id,
            collection_id=collection_id_uuid,
        )
        await service.providers.database.documents_handler.set_workflow_status(
            id=collection_id_uuid,
            status_type="graph_sync_status",
            status=GraphConstructionStatus.OUTDATED,
        )
        await service.providers.database.documents_handler.set_workflow_status(
            id=collection_id_uuid,
            status_type="graph_cluster_status",
            status=GraphConstructionStatus.OUTDATED,  # NOTE - we should actually check that cluster has been made first, if not it should be PENDING still
        )
    else:
        for collection_id in collection_ids:
            try:
                # First check if collection exists
                collection_exists = await service.providers.database.collections_handler.collection_exists(collection_id)
                if not collection_exists:
                    # Only create if it doesn't exist
                    name = document_info.title or "N/A"
                    description = ""
                    await service.providers.database.collections_handler.create_collection(
                        owner_id=document_info.owner_id,
                        name=name,
                        description=description,
                        collection_id=collection_id,
                    )
                    await service.providers.database.graphs_handler.create(
                        collection_id=collection_id,
                        name=name,
                        description=description,
                        graph_id=collection_id,
                    )
            except Exception as e:
                logger.warning(
                    f"Warning, could not create/verify collection with error: {str(e)}"
                )

            # Always try to assign document regardless of collection creation result
            collection_id_uuid = UUID(str(collection_id))
            await service.providers.database.collections_handler.assign_document_to_collection_relational(
                document_id=document_info.id,
                collection_id=collection_id_uuid,
            )
            await service.providers.database.collections_handler.update_document_count_recursively(
                collection_id_uuid, increment=True, change_amount=1
            )
            await service.providers.database.chunks_handler.assign_document_chunks_to_collection(
                document_id=document_info.id,
                collection_id=collection_id_uuid,
            )
            await service.providers.database.documents_handler.set_workflow_status(
                id=collection_id_uuid,
                status_type="graph_sync_status",
                status=GraphConstructionStatus.OUTDATED,
            )
            await service.providers.database.documents_handler.set_workflow_status(
                id=collection_id_uuid,
                status_type="graph_cluster_status",
                status=GraphConstructionStatus.OUTDATED,  # NOTE - we should actually check that cluster has been made first, if not it should be PENDING still
            ) 