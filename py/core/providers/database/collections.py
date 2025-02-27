import csv
import json
import logging
import tempfile
import os
from typing import IO, Any, Optional
from uuid import UUID, uuid4

from asyncpg.exceptions import UniqueViolationError
from fastapi import HTTPException

from core.base import (
    DatabaseConfig,
    GraphExtractionStatus,
    Handler,
    R2RException,
    generate_default_user_collection_id,
)
from core.base.abstractions import (
    DocumentResponse,
    DocumentType,
    IngestionStatus,
)
from core.base.api.models import CollectionResponse

from .base import PostgresConnectionManager

logger = logging.getLogger()


class PostgresCollectionsHandler(Handler):
    TABLE_NAME = "collections"

    def __init__(
        self,
        project_name: str,
        connection_manager: PostgresConnectionManager,
        config: DatabaseConfig,
    ):
        self.config = config
        super().__init__(project_name, connection_manager)

    async def create_tables(self) -> None:
        """Create the collections table if it doesn't exist."""
        query = f"""
            CREATE TABLE IF NOT EXISTS {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} (
                id UUID PRIMARY KEY,
                owner_id UUID NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                theme TEXT,
                icon TEXT,
                parent_id UUID REFERENCES {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}(id) ON DELETE CASCADE,
                subcollections UUID[] DEFAULT ARRAY[]::UUID[],
                graph_sync_status TEXT DEFAULT 'pending',
                graph_cluster_status TEXT DEFAULT 'pending',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT valid_parent CHECK (parent_id != id)
            );

            -- Create index on parent_id for faster hierarchy traversal
            CREATE INDEX IF NOT EXISTS idx_{self.project_name}_collections_parent_id 
            ON {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} (parent_id);

            -- Create index on subcollections array for faster subcollection lookups
            CREATE INDEX IF NOT EXISTS idx_{self.project_name}_collections_subcollections 
            ON {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} USING GIN (subcollections);
        """
        await self.connection_manager.execute_query(query)

    async def collection_exists(self, collection_id: UUID) -> bool:
        """Check if a collection exists."""
        query = f"""
            SELECT 1 FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
            WHERE id = $1
        """
        result = await self.connection_manager.fetchrow_query(
            query, [collection_id]
        )
        return result is not None

    async def create_collection(
        self,
        owner_id: UUID,
        name: Optional[str] = None,
        description: str = "",
        collection_id: Optional[UUID] = None,
        theme: Optional[str] = None,
        icon: Optional[str] = None,
        parent_id: Optional[UUID] = None,
    ) -> CollectionResponse:
        """Create a new collection with optional subcollections."""
        logger.info(
            "Creating collection with params: owner_id=%s, name=%s, description=%s, collection_id=%s, theme=%s, icon=%s, parent_id=%s",
            owner_id, name, description, collection_id, theme, icon, parent_id
        )

        if not name and not collection_id:
            name = self.config.default_collection_name
            collection_id = uuid4()
            logger.info("Using default collection name=%s and generated collection_id=%s", name, collection_id)

        # Set default theme and icon if not provided
        theme = theme or os.getenv("R2R_DEFAULT_COLLECTION_THEME", "#a855f7")
        icon = icon or os.getenv("R2R_DEFAULT_COLLECTION_ICON", "Book")
        logger.info("Using theme=%s and icon=%s", theme, icon)

        # Check if collection with this ID already exists
        collection_id = collection_id or uuid4()
        
        # Retry up to 3 times with new UUIDs if there's a conflict
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            if await self.collection_exists(collection_id):
                logger.warning("Collection with ID %s already exists, generating a new ID", collection_id)
                collection_id = uuid4()
                logger.info("Generated new collection ID: %s", collection_id)
                retry_count += 1
            else:
                break
        
        if retry_count >= max_retries:
            logger.error("Failed to generate a unique collection ID after %s attempts", max_retries)
            raise R2RException(
                message="Failed to generate a unique collection ID",
                status_code=500,
            )

        query = f"""
            INSERT INTO {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
            (id, owner_id, name, description, theme, icon, parent_id, subcollections)
            VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::uuid, $8::uuid[])
            RETURNING id, owner_id, name, description, theme, icon, parent_id, subcollections,
                      graph_sync_status, graph_cluster_status, created_at, updated_at
        """
        
        # Maximum number of retries for unique violation errors
        max_insert_retries = 5
        insert_retry_count = 0
        original_name = name
        
        while insert_retry_count < max_insert_retries:
            try:
                params = [
                    str(collection_id),
                    str(owner_id),
                    name,
                    description,
                    theme,
                    icon,
                    str(parent_id) if parent_id else None,
                    [],  # Initialize empty subcollections array
                ]
                
                result = await self.connection_manager.fetchrow_query(
                    query=query,
                    params=params,
                )
                
                if not result:
                    logger.error("Failed to create collection - no result returned")
                    raise R2RException(
                        status_code=404, message="Collection not found"
                    )

                # Get the complete collection details using get_collection_by_id
                collection = await self.get_collection_by_id(collection_id)
                if not collection:
                    logger.error("Failed to retrieve created collection details")
                    raise R2RException(
                        status_code=404, message="Collection not found"
                    )
                
                logger.info(
                    "Created collection: id=%s, name=%s, owner_id=%s",
                    collection.id,
                    collection.name,
                    collection.owner_id,
                )

                # Create default subcollections if this is a root collection (no parent_id) and has a name
                # Only create subcollections if a name was explicitly provided (not None or empty string)
                # AND the name is not equal to the default_collection_name
                if not parent_id and name and name.strip() and name != self.config.default_collection_name:
                    logger.info("Creating default subcollections for root collection id=%s", collection.id)
                    
                    # Create main subcollection
                    main_subcoll_name = os.getenv("R2R_MAIN_SUBCOLLECTION_NAME", "Class 1")
                    main_subcoll_desc = os.getenv("R2R_MAIN_SUBCOLLECTION_DESC", "Your first class for this course")
                    main_subcoll_theme = os.getenv("R2R_MAIN_SUBCOLLECTION_THEME", "#a855f7")
                    main_subcoll_icon = os.getenv("R2R_MAIN_SUBCOLLECTION_ICON", "Book")
                    
                    # Generate a new UUID for the main subcollection
                    main_subcoll_id = uuid4()
                    logger.info("Creating main subcollection with name=%s under parent_id=%s with ID=%s", 
                               main_subcoll_name, collection.id, main_subcoll_id)
                    
                    # Try to create the main subcollection with retries for name uniqueness
                    main_subcoll = None
                    name_retry_count = 0
                    max_name_retries = 5
                    current_main_name = main_subcoll_name
                    
                    while name_retry_count < max_name_retries and not main_subcoll:
                        try:
                            main_subcoll = await self.create_collection(
                                owner_id=owner_id,
                                name=current_main_name,
                                description=main_subcoll_desc,
                                collection_id=main_subcoll_id,
                                theme=main_subcoll_theme,
                                icon=main_subcoll_icon,
                                parent_id=collection.id,
                            )
                            break
                        except HTTPException as e:
                            if "unique constraint" in str(e).lower() or "already exists" in str(e).lower():
                                # If name conflict, append a suffix and retry
                                name_retry_count += 1
                                current_main_name = f"{main_subcoll_name} ({name_retry_count})"
                                main_subcoll_id = uuid4()  # Generate a new ID for the retry
                                logger.info("Name conflict for main subcollection, retrying with name=%s and ID=%s", 
                                           current_main_name, main_subcoll_id)
                            else:
                                # If it's a different error, re-raise it
                                raise
                    
                    if not main_subcoll:
                        logger.error("Failed to create main subcollection after %s name retries", max_name_retries)
                        raise R2RException(
                            message="Failed to create main subcollection after multiple retries",
                            status_code=500,
                        )
                    
                    # Add main subcollection to parent's subcollections array
                    update_query = f"""
                        UPDATE {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                        SET subcollections = array_append(subcollections, $1::uuid)
                        WHERE id = $2::uuid
                        RETURNING id
                    """
                    await self.connection_manager.execute_query(
                        update_query,
                        [str(main_subcoll.id), str(collection.id)]
                    )
                    
                    logger.info(
                        "Created main subcollection: id=%s, name=%s",
                        main_subcoll.id,
                        main_subcoll.name
                    )

                    # Create standard subcollections under the main subcollection
                    subcollections_config = [
                        {
                            "name": os.getenv("R2R_TEXTBOOKS_NAME", "Textbooks"),
                            "desc": os.getenv("R2R_TEXTBOOKS_DESC", "General documents collection"),
                            "theme": os.getenv("R2R_TEXTBOOKS_THEME", "#a855f7"),
                            "icon": os.getenv("R2R_TEXTBOOKS_ICON", "BookOpen")
                        },
                        {
                            "name": os.getenv("R2R_ASSIGNMENTS_NAME", "Assignments"),
                            "desc": os.getenv("R2R_ASSIGNMENTS_DESC", "Assignment instructions and solutions"),
                            "theme": os.getenv("R2R_ASSIGNMENTS_THEME", "#a855f7"),
                            "icon": os.getenv("R2R_ASSIGNMENTS_ICON", "ClipboardList")
                        },
                        {
                            "name": os.getenv("R2R_NOTES_NAME", "Notes"),
                            "desc": os.getenv("R2R_NOTES_DESC", "Class notes eg. written notes"),
                            "theme": os.getenv("R2R_NOTES_THEME", "#a855f7"),
                            "icon": os.getenv("R2R_NOTES_ICON", "Pencil")
                        }
                    ]

                    for config in subcollections_config:
                        # Generate a unique ID for each standard subcollection
                        subcoll_id = uuid4()
                        subcoll_name = config["name"]
                        logger.info("Creating standard subcollection with name=%s under parent_id=%s with ID=%s", 
                                   subcoll_name, main_subcoll.id, subcoll_id)
                        
                        # Try to create the standard subcollection with retries for name uniqueness
                        subcoll = None
                        name_retry_count = 0
                        current_subcoll_name = subcoll_name
                        
                        while name_retry_count < max_name_retries and not subcoll:
                            try:
                                subcoll = await self.create_collection(
                                    owner_id=owner_id,
                                    name=current_subcoll_name,
                                    description=config["desc"],
                                    collection_id=subcoll_id,
                                    theme=config["theme"],
                                    icon=config["icon"],
                                    parent_id=main_subcoll.id,
                                )
                                break
                            except HTTPException as e:
                                if "unique constraint" in str(e).lower() or "already exists" in str(e).lower():
                                    # If name conflict, append a suffix and retry
                                    name_retry_count += 1
                                    current_subcoll_name = f"{subcoll_name} ({name_retry_count})"
                                    subcoll_id = uuid4()  # Generate a new ID for the retry
                                    logger.info("Name conflict for standard subcollection, retrying with name=%s and ID=%s", 
                                               current_subcoll_name, subcoll_id)
                                else:
                                    # If it's a different error, re-raise it
                                    raise
                        
                        if not subcoll:
                            logger.error("Failed to create standard subcollection after %s name retries", max_name_retries)
                            continue  # Skip this subcollection and try the next one
                        
                        # Add subcollection to main subcollection's subcollections array
                        await self.connection_manager.execute_query(
                            update_query,
                            [str(subcoll.id), str(main_subcoll.id)]
                        )
                        
                        logger.info(
                            "Created standard subcollection: id=%s, name=%s",
                            subcoll.id,
                            subcoll.name
                        )
                else:
                    logger.info("Skipping subcollection creation: parent_id=%s, name=%s", parent_id, name)

                # Get the final collection details with all subcollections
                final_collection = await self.get_collection_by_id(collection_id)
                if not final_collection:
                    logger.error("Failed to retrieve final collection details")
                    raise R2RException(
                        status_code=404, message="Collection not found"
                    )

                return final_collection
                
            except UniqueViolationError as e:
                if "unique_owner_collection_name" in str(e):
                    # This is a name conflict, not an ID conflict
                    insert_retry_count += 1
                    name = f"{original_name} ({insert_retry_count})"
                    logger.warning("Name conflict for collection, retrying with name=%s", name)
                else:
                    # This is an ID conflict
                    logger.warning("Unique violation error for collection_id=%s, retrying with a new ID", collection_id)
                    collection_id = uuid4()
                    logger.info("Generated new collection ID for retry: %s", collection_id)
                    insert_retry_count += 1
                
                if insert_retry_count >= max_insert_retries:
                    logger.error("Failed to create collection after %s retries - unique violation errors", max_insert_retries)
                    raise R2RException(
                        message="Failed to create collection after multiple retries",
                        status_code=500,
                    )
            except Exception as e:
                logger.error("Failed to create collection: %s", str(e))
                raise HTTPException(
                    status_code=500,
                    detail=f"An error occurred while creating the collection: {e}",
                ) from e

    async def update_collection(
        self,
        collection_id: UUID,
        name: Optional[str] = None,
        description: Optional[str] = None,
        theme: Optional[str] = None,
        icon: Optional[str] = None,
        parent_id: Optional[UUID] = None,
    ) -> CollectionResponse:
        """Update an existing collection."""
        if not await self.collection_exists(collection_id):
            raise R2RException(status_code=404, message="Collection not found")

        # Prevent circular parent references
        if parent_id:
            current = parent_id
            visited = {collection_id}
            while current:
                if current in visited:
                    raise R2RException(
                        status_code=400,
                        message="Circular parent reference detected"
                    )
                visited.add(current)
                parent_result = await self.connection_manager.fetchrow_query(
                    f"SELECT parent_id FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} WHERE id = $1",
                    [current]
                )
                current = parent_result["parent_id"] if parent_result else None

        update_fields = []
        params: list = []
        param_index = 1

        if name is not None:
            update_fields.append(f"name = ${param_index}")
            params.append(name)
            param_index += 1

        if description is not None:
            update_fields.append(f"description = ${param_index}")
            params.append(description)
            param_index += 1

        if theme is not None:
            update_fields.append(f"theme = ${param_index}")
            params.append(theme)
            param_index += 1

        if icon is not None:
            update_fields.append(f"icon = ${param_index}")
            params.append(icon)
            param_index += 1

        if parent_id is not None:
            update_fields.append(f"parent_id = ${param_index}")
            params.append(parent_id)
            param_index += 1

        if not update_fields:
            raise R2RException(status_code=400, message="No fields to update")

        update_fields.append("updated_at = NOW()")
        params.append(collection_id)

        query = f"""
            WITH updated_collection AS (
                UPDATE {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                SET {", ".join(update_fields)}
                WHERE id = ${param_index}
                RETURNING id, owner_id, name, description, theme, icon, parent_id, graph_sync_status, graph_cluster_status, created_at, updated_at
            )
            SELECT
                uc.*,
                COUNT(DISTINCT u.id) FILTER (WHERE u.id IS NOT NULL) as user_count,
                COUNT(DISTINCT d.id) FILTER (WHERE d.id IS NOT NULL) as document_count
            FROM updated_collection uc
            LEFT JOIN {self._get_table_name("users")} u ON uc.id = ANY(u.collection_ids)
            LEFT JOIN {self._get_table_name("documents")} d ON uc.id = ANY(d.collection_ids)
            GROUP BY uc.id, uc.owner_id, uc.name, uc.description, uc.theme, uc.icon, uc.parent_id, uc.graph_sync_status, uc.graph_cluster_status, uc.created_at, uc.updated_at
        """
        try:
            result = await self.connection_manager.fetchrow_query(
                query, params
            )
            if not result:
                raise R2RException(
                    status_code=404, message="Collection not found"
                )

            return CollectionResponse(
                id=result["id"],
                owner_id=result["owner_id"],
                name=result["name"],
                description=result["description"],
                theme=result["theme"],
                icon=result["icon"],
                parent_id=result["parent_id"],
                graph_sync_status=result["graph_sync_status"],
                graph_cluster_status=result["graph_cluster_status"],
                created_at=result["created_at"],
                updated_at=result["updated_at"],
                user_count=result["user_count"],
                document_count=result["document_count"],
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred while updating the collection: {e}",
            ) from e

    async def delete_collection_relational(self, collection_id: UUID) -> None:
        # Remove collection_id from users
        user_update_query = f"""
            UPDATE {self._get_table_name("users")}
            SET collection_ids = array_remove(collection_ids, $1)
            WHERE $1 = ANY(collection_ids)
        """
        await self.connection_manager.execute_query(
            user_update_query, [collection_id]
        )

        # Remove collection_id from documents
        document_update_query = f"""
            WITH updated AS (
                UPDATE {self._get_table_name("documents")}
                SET collection_ids = array_remove(collection_ids, $1)
                WHERE $1 = ANY(collection_ids)
                RETURNING 1
            )
            SELECT COUNT(*) AS affected_rows FROM updated
        """
        await self.connection_manager.fetchrow_query(
            document_update_query, [collection_id]
        )

        # Delete the collection
        delete_query = f"""
            DELETE FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
            WHERE id = $1
            RETURNING id
        """
        deleted = await self.connection_manager.fetchrow_query(
            delete_query, [collection_id]
        )

        if not deleted:
            raise R2RException(status_code=404, message="Collection not found")

    async def get_collection_by_id(self, collection_id: UUID) -> Optional[CollectionResponse]:
        """Fetch a collection by its UUID.
        
        Args:
            collection_id (UUID): The UUID of the collection to retrieve
            
        Returns:
            Optional[CollectionResponse]: The collection if found, None otherwise
        """
        query = f"""
            WITH RECURSIVE collection_tree AS (
                -- Base case: get the requested collection
                SELECT 
                    c.*,
                    COUNT(DISTINCT u.id) FILTER (WHERE u.id IS NOT NULL) as user_count,
                    COUNT(DISTINCT d.id) FILTER (WHERE d.id IS NOT NULL) as document_count
                FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} c
                LEFT JOIN {self._get_table_name("users")} u ON c.id = ANY(u.collection_ids)
                LEFT JOIN {self._get_table_name("documents")} d ON c.id = ANY(d.collection_ids)
                WHERE c.id = $1::uuid
                GROUP BY c.id, c.owner_id, c.name, c.description, c.theme, c.icon, 
                         c.parent_id, c.graph_sync_status, c.graph_cluster_status, 
                         c.created_at, c.updated_at, c.subcollections
            )
            SELECT * FROM collection_tree
        """
        
        result = await self.connection_manager.fetchrow_query(query, [str(collection_id)])
        
        if not result:
            return None
            
        collection = CollectionResponse(
            id=result["id"],
            owner_id=result["owner_id"],
            name=result["name"],
            description=result["description"],
            theme=result["theme"],
            icon=result["icon"],
            parent_id=result["parent_id"],
            graph_sync_status=result["graph_sync_status"],
            graph_cluster_status=result["graph_cluster_status"],
            created_at=result["created_at"],
            updated_at=result["updated_at"],
            user_count=result["user_count"],
            document_count=result["document_count"],
            subcollections=result.get("subcollections", []) or [],
            subcollection_details=[]
        )
        
        # Fetch subcollection details if there are any
        if collection.subcollections:
            for subcoll_id in collection.subcollections:
                subcoll = await self.get_collection_by_id(subcoll_id)
                if subcoll:
                    collection.subcollection_details.append(subcoll)
        
        return collection

    async def documents_in_collection(
        self, collection_id: UUID, offset: int, limit: int
    ) -> dict[str, list[DocumentResponse] | int]:
        """Get all documents in a specific collection with pagination.

        Args:
            collection_id (UUID): The ID of the collection to get documents from.
            offset (int): The number of documents to skip.
            limit (int): The maximum number of documents to return.
        Returns:
            List[DocumentResponse]: A list of DocumentResponse objects representing the documents in the collection.
        Raises:
            R2RException: If the collection doesn't exist.
        """
        collection = await self.get_collection_by_id(collection_id)
        if not collection:
            raise R2RException(status_code=404, message="Collection not found")

        query = f"""
            SELECT d.id, d.owner_id, d.type, d.metadata, d.title, d.version,
                d.size_in_bytes, d.ingestion_status, d.extraction_status, d.created_at, d.updated_at, d.summary,
                COUNT(*) OVER() AS total_entries
            FROM {self._get_table_name("documents")} d
            WHERE $1::uuid = ANY(d.collection_ids)
            ORDER BY d.created_at DESC
            OFFSET $2
        """

        conditions = [str(collection_id), offset]
        if limit != -1:
            query += " LIMIT $3"
            conditions.append(limit)

        results = await self.connection_manager.fetch_query(query, conditions)
        documents = [
            DocumentResponse(
                id=row["id"],
                collection_ids=[collection_id],
                owner_id=row["owner_id"],
                document_type=DocumentType(row["type"]),
                metadata=json.loads(row["metadata"]),
                title=row["title"],
                version=row["version"],
                size_in_bytes=row["size_in_bytes"],
                ingestion_status=IngestionStatus(row["ingestion_status"]),
                extraction_status=GraphExtractionStatus(
                    row["extraction_status"]
                ),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                summary=row["summary"],
            )
            for row in results
        ]
        total_entries = results[0]["total_entries"] if results else 0

        return {"results": documents, "total_entries": total_entries}

    async def get_collections_overview(
        self,
        offset: int,
        limit: int,
        filter_user_ids: Optional[list[UUID]] = None,
        filter_document_ids: Optional[list[UUID]] = None,
        filter_collection_ids: Optional[list[UUID]] = None,
    ) -> dict[str, list[CollectionResponse] | int]:
        conditions = []
        params: list[Any] = []
        param_index = 1

        if filter_user_ids:
            conditions.append(f"""
                c.id IN (
                    SELECT unnest(collection_ids)
                    FROM {self._get_table_name("users")}
                    WHERE id = ANY(${param_index})
                )
            """)
            params.append(filter_user_ids)
            param_index += 1

        if filter_document_ids:
            conditions.append(f"""
                c.id IN (
                    SELECT unnest(collection_ids)
                    FROM {self._get_table_name("documents")}
                    WHERE id = ANY(${param_index})
                )
            """)
            params.append(filter_document_ids)
            param_index += 1

        if filter_collection_ids:
            conditions.append(f"c.id = ANY(${param_index})")
            params.append(filter_collection_ids)
            param_index += 1

        where_clause = (
            f"WHERE {' AND '.join(conditions)}" if conditions else ""
        )

        # Get root collections first
        root_collections_query = f"""
            SELECT 
                c.*,
                COUNT(DISTINCT u.id) FILTER (WHERE u.id IS NOT NULL) as user_count
            FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} c
            LEFT JOIN {self._get_table_name("users")} u ON c.id = ANY(u.collection_ids)
            WHERE c.parent_id IS NULL
            {f'AND {" AND ".join(conditions)}' if conditions else ''}
            GROUP BY c.id, c.owner_id, c.name, c.description, c.theme, c.icon, 
                     c.parent_id, c.graph_sync_status, c.graph_cluster_status, 
                     c.created_at, c.updated_at, c.document_count
            ORDER BY c.created_at DESC
        """

        try:
            root_results = await self.connection_manager.fetch_query(root_collections_query, params)
            
            if not root_results:
                return {"results": [], "total_entries": 0}

            async def get_collection_details(collection_id: UUID) -> CollectionResponse:
                # Get collection details including counts
                query = f"""
                    SELECT 
                        c.*,
                        COUNT(DISTINCT u.id) FILTER (WHERE u.id IS NOT NULL) as user_count
                    FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} c
                    LEFT JOIN {self._get_table_name("users")} u ON c.id = ANY(u.collection_ids)
                    WHERE c.id = $1
                    GROUP BY c.id, c.owner_id, c.name, c.description, c.theme, c.icon, 
                             c.parent_id, c.graph_sync_status, c.graph_cluster_status, 
                             c.created_at, c.updated_at, c.document_count
                """
                result = await self.connection_manager.fetchrow_query(query, [collection_id])
                
                collection = CollectionResponse(
                    id=result["id"],
                    owner_id=result["owner_id"],
                    name=result["name"],
                    description=result["description"],
                    theme=result["theme"],
                    icon=result["icon"],
                    parent_id=result["parent_id"],
                    graph_sync_status=result["graph_sync_status"],
                    graph_cluster_status=result["graph_cluster_status"],
                    created_at=result["created_at"],
                    updated_at=result["updated_at"],
                    user_count=result["user_count"],
                    document_count=result["document_count"],
                    subcollections=result.get("subcollections", []) or [],
                    subcollection_details=[]
                )
                
                # Recursively get subcollection details
                for subcoll_id in collection.subcollections:
                    subcoll = await get_collection_details(subcoll_id)
                    collection.subcollection_details.append(subcoll)
                
                return collection

            # Build collection hierarchy for root collections
            root_collections = []
            for row in root_results:
                collection = await get_collection_details(row["id"])
                root_collections.append(collection)

            # Apply pagination to root collections
            paginated_roots = root_collections[offset:offset + limit] if limit != -1 else root_collections[offset:]
            total_entries = len(root_collections)

            return {"results": paginated_roots, "total_entries": total_entries}
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred while fetching collections: {e}",
            ) from e

    async def update_document_count_recursively(
        self,
        collection_id: UUID,
        increment: bool = True,
        change_amount: int = 1
    ) -> None:
        """Update document count for a collection and all its ancestors.

        Args:
            collection_id (UUID): The ID of the collection to start updating from
            increment (bool): True to increment, False to decrement
            change_amount (int): Amount to change the count by (default: 1)
        """
        query = f"""
            WITH RECURSIVE collection_hierarchy AS (
                -- Base case: start with the given collection
                SELECT id, parent_id
                FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
                WHERE id = $1::uuid

                UNION

                -- Recursive case: get all ancestors
                SELECT c.id, c.parent_id
                FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} c
                INNER JOIN collection_hierarchy ch ON ch.parent_id = c.id
            )
            UPDATE {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} c
            SET document_count = document_count {'+' if increment else '-'} $2
            WHERE c.id IN (SELECT id FROM collection_hierarchy)
        """
        await self.connection_manager.execute_query(
            query, [str(collection_id), change_amount]
        )

    async def assign_document_to_collection_relational(
        self,
        document_id: UUID,
        collection_id: UUID,
    ) -> UUID:
        """Assign a document to a collection.

        Args:
            document_id (UUID): The ID of the document to assign.
            collection_id (UUID): The ID of the collection to assign the document to.

        Raises:
            R2RException: If the collection doesn't exist, if the document is not found,
                        or if there's a database error.
        """
        try:
            # Check if collection exists using get_collection_by_id
            collection = await self.get_collection_by_id(collection_id)
            if not collection:
                raise R2RException(
                    status_code=404, message="Collection not found"
                )

            # First, check if the document exists
            document_check_query = f"""
                SELECT 1 FROM {self._get_table_name("documents")}
                WHERE id = $1::uuid
            """
            document_exists = await self.connection_manager.fetchrow_query(
                document_check_query, [str(document_id)]
            )

            if not document_exists:
                raise R2RException(
                    status_code=404, message="Document not found"
                )

            # If document exists, proceed with the assignment
            assign_query = f"""
                UPDATE {self._get_table_name("documents")}
                SET collection_ids = array_append(collection_ids, $1::uuid)
                WHERE id = $2::uuid AND NOT ($1::uuid = ANY(collection_ids))
                RETURNING id
            """
            result = await self.connection_manager.fetchrow_query(
                assign_query, [str(collection_id), str(document_id)]
            )

            if not result:
                # Document exists but was already assigned to the collection
                raise R2RException(
                    status_code=409,
                    message="Document is already assigned to the collection",
                )

            # Update document count recursively for the collection and its ancestors
            await self.update_document_count_recursively(collection_id, increment=True)

            return collection_id

        except R2RException:
            # Re-raise R2RExceptions as they are already handled
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred while assigning the document to the collection: {e}",
            ) from e

    async def remove_document_from_collection_relational(
        self, document_id: UUID, collection_id: UUID
    ) -> None:
        """Remove a document from a collection.

        Args:
            document_id (UUID): The ID of the document to remove.
            collection_id (UUID): The ID of the collection to remove the document from.

        Raises:
            R2RException: If the collection doesn't exist or if the document is not in the collection.
        """
        # Check if collection exists using get_collection_by_id
        collection = await self.get_collection_by_id(collection_id)
        if not collection:
            raise R2RException(status_code=404, message="Collection not found")

        query = f"""
            UPDATE {self._get_table_name("documents")}
            SET collection_ids = array_remove(collection_ids, $1::uuid)
            WHERE id = $2::uuid AND $1::uuid = ANY(collection_ids)
            RETURNING id
        """
        result = await self.connection_manager.fetchrow_query(
            query, [str(collection_id), str(document_id)]
        )

        if not result:
            raise R2RException(
                status_code=404,
                message="Document not found in the specified collection",
            )

        # Update document count recursively for the collection and its ancestors
        await self.update_document_count_recursively(collection_id, increment=False)

    async def decrement_collection_document_count(
        self, collection_id: UUID, decrement_by: int = 1
    ) -> None:
        """Decrement the document count for a collection.

        Args:
            collection_id (UUID): The ID of the collection to update
            decrement_by (int): Number to decrease the count by (default: 1)
        """
        collection_query = f"""
            UPDATE {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)}
            SET document_count = document_count - $1
            WHERE id = $2::uuid
        """
        await self.connection_manager.execute_query(
            collection_query, [decrement_by, str(collection_id)]
        )

    async def export_to_csv(
        self,
        columns: Optional[list[str]] = None,
        filters: Optional[dict] = None,
        include_header: bool = True,
    ) -> tuple[str, IO]:
        """Creates a CSV file from the PostgreSQL data and returns the path to
        the temp file."""
        valid_columns = {
            "id",
            "owner_id",
            "name",
            "description",
            "graph_sync_status",
            "graph_cluster_status",
            "created_at",
            "updated_at",
            "user_count",
            "document_count",
        }

        if not columns:
            columns = list(valid_columns)
        elif invalid_cols := set(columns) - valid_columns:
            raise ValueError(f"Invalid columns: {invalid_cols}")

        select_stmt = f"""
            SELECT
                id::text,
                owner_id::text,
                name,
                description,
                graph_sync_status,
                graph_cluster_status,
                to_char(created_at, 'YYYY-MM-DD HH24:MI:SS') AS created_at,
                to_char(updated_at, 'YYYY-MM-DD HH24:MI:SS') AS updated_at,
                user_count,
                document_count
            FROM {self._get_table_name(self.TABLE_NAME)}
        """

        params = []
        if filters:
            conditions = []
            param_index = 1

            for field, value in filters.items():
                if field not in valid_columns:
                    continue

                if isinstance(value, dict):
                    for op, val in value.items():
                        if op == "$eq":
                            conditions.append(f"{field} = ${param_index}")
                            params.append(val)
                            param_index += 1
                        elif op == "$gt":
                            conditions.append(f"{field} > ${param_index}")
                            params.append(val)
                            param_index += 1
                        elif op == "$lt":
                            conditions.append(f"{field} < ${param_index}")
                            params.append(val)
                            param_index += 1
                else:
                    # Direct equality
                    conditions.append(f"{field} = ${param_index}")
                    params.append(value)
                    param_index += 1

            if conditions:
                select_stmt = f"{select_stmt} WHERE {' AND '.join(conditions)}"

        select_stmt = f"{select_stmt} ORDER BY created_at DESC"

        temp_file = None
        try:
            temp_file = tempfile.NamedTemporaryFile(
                mode="w", delete=True, suffix=".csv"
            )
            writer = csv.writer(temp_file, quoting=csv.QUOTE_ALL)

            async with self.connection_manager.pool.get_connection() as conn:  # type: ignore
                async with conn.transaction():
                    cursor = await conn.cursor(select_stmt, *params)

                    if include_header:
                        writer.writerow(columns)

                    chunk_size = 1000
                    while True:
                        rows = await cursor.fetch(chunk_size)
                        if not rows:
                            break
                        for row in rows:
                            row_dict = {
                                "id": row[0],
                                "owner_id": row[1],
                                "name": row[2],
                                "description": row[3],
                                "graph_sync_status": row[4],
                                "graph_cluster_status": row[5],
                                "created_at": row[6],
                                "updated_at": row[7],
                                "user_count": row[8],
                                "document_count": row[9],
                            }
                            writer.writerow([row_dict[col] for col in columns])

            temp_file.flush()
            return temp_file.name, temp_file

        except Exception as e:
            if temp_file:
                temp_file.close()
            raise HTTPException(
                status_code=500,
                detail=f"Failed to export data: {str(e)}",
            ) from e

    async def get_collection_by_name(
        self, owner_id: UUID, name: str
    ) -> Optional[CollectionResponse]:
        """Fetch a collection by owner_id + name combination.

        Return None if not found.
        """
        query = f"""
            WITH RECURSIVE collection_tree AS (
                -- Base case: get the requested collection
                SELECT 
                    c.*,
                    ARRAY[]::uuid[] as subcollections,
                    0 as level
                FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} c
                WHERE c.owner_id = $1 AND c.name = $2
                
                UNION ALL
                
                -- Recursive case: get child collections
                SELECT 
                    c.*,
                    ARRAY[]::uuid[] as subcollections,
                    ct.level + 1
                FROM {self._get_table_name(PostgresCollectionsHandler.TABLE_NAME)} c
                JOIN collection_tree ct ON c.parent_id = ct.id
            )
            SELECT 
                ct.*,
                COUNT(DISTINCT u.id) FILTER (WHERE u.id IS NOT NULL) as user_count,
                COUNT(DISTINCT d.id) FILTER (WHERE d.id IS NOT NULL) as document_count
            FROM collection_tree ct
            LEFT JOIN {self._get_table_name("users")} u ON ct.id = ANY(u.collection_ids)
            LEFT JOIN {self._get_table_name("documents")} d ON ct.id = ANY(d.collection_ids)
            GROUP BY ct.id, ct.owner_id, ct.name, ct.description, ct.theme, ct.icon, 
                     ct.parent_id, ct.graph_sync_status, ct.graph_cluster_status, 
                     ct.created_at, ct.updated_at, ct.subcollections, ct.level
            ORDER BY ct.level ASC
        """
        
        results = await self.connection_manager.fetch_query(query, [owner_id, name])
        
        if not results:
            raise R2RException(
                status_code=404,
                message="No collection found with the specified name",
            )
            
        # Build collection hierarchy
        collections_by_id = {}
        root_collection = None
        
        # First pass: Create CollectionResponse objects
        for row in results:
            collection = CollectionResponse(
                id=row["id"],
                owner_id=row["owner_id"],
                name=row["name"],
                description=row["description"],
                theme=row["theme"],
                icon=row["icon"],
                parent_id=row["parent_id"],
                graph_sync_status=row["graph_sync_status"],
                graph_cluster_status=row["graph_cluster_status"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                user_count=row["user_count"],
                document_count=row["document_count"],
                subcollections=[],
                subcollection_details=[]
            )
            collections_by_id[row["id"]] = collection
            if row["level"] == 0:  # This is our requested collection
                root_collection = collection

        # Second pass: Build hierarchy
        for collection in collections_by_id.values():
            if collection.parent_id:
                parent = collections_by_id.get(collection.parent_id)
                if parent:
                    parent.subcollections.append(collection.id)
                    parent.subcollection_details.append(collection)

        return root_collection
